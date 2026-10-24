/*
 * This file is part of the OpenKinect Project. http://www.openkinect.org
 *
 * Copyright (c) 2011 individual OpenKinect contributors. See the CONTRIB file
 * for details.
 *
 * This code is licensed to you under the terms of the Apache License, version
 * 2.0, or, at your option, the terms of the GNU General Public License,
 * version 2.0. See the APACHE20 and GPL2 files for the text of the licenses,
 * or the following URLs:
 * http://www.apache.org/licenses/LICENSE-2.0
 * http://www.gnu.org/licenses/gpl-2.0.txt
 *
 * If you redistribute this file in source form, modified or unmodified, you
 * may:
 *   1) Leave this header intact and distribute it under the same terms,
 *      accompanying it with the APACHE20 and GPL20 files, or
 *   2) Delete the Apache 2.0 clause and accompany it with the GPL2 file, or
 *   3) Delete the GPL v2 clause and accompany it with the APACHE20 file
 * In all cases you must keep the copyright notice intact and include a copy
 * of the CONTRIB file.
 *
 * Binary distributions must follow the binary distribution requirements of
 * either License.
 */

/** @file Depthws.cpp Streams depth frames to a websocket server as binary messages. */

#include <iostream>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <mutex>
#include <atomic>
#include <chrono>
#include <signal.h>

#include <libwebsockets.h>

#include <libfreenect2/libfreenect2.hpp>
#include <libfreenect2/frame_listener.hpp>
#include <libfreenect2/packet_pipeline.h>
#include <libfreenect2/logger.h>

static const char *DEFAULT_WS_HOST = "localhost";
static const int DEFAULT_WS_PORT = 8080;

volatile sig_atomic_t depthws_shutdown = 0; ///< Whether the running application should shut down.

void sigint_handler(int s)
{
  depthws_shutdown = 1;
}

/** State shared between the websocket service loop (main thread) and the depth listener. */
struct WsSession
{
  enum State { Connecting, Open, Closing, Closed };

  lws_context *context;
  lws *wsi;
  State state;
  size_t frames_sent;
  lws_sorted_usec_list_t tick;

  std::mutex mutex;
  bool has_pending;
  std::vector<unsigned char> pending; ///< LWS_PRE bytes of headroom followed by the frame payload.
  std::vector<unsigned char> sending;

  WsSession() : context(NULL), wsi(NULL), state(Connecting), frames_sent(0), has_pending(false)
  {
    std::memset(&tick, 0, sizeof(tick));
  }
};

static WsSession *ws_session = NULL;

static int ws_callback(lws *wsi, enum lws_callback_reasons reason, void *user, void *in, size_t len)
{
  switch (reason)
  {
  case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
    std::cerr << "websocket connection error: " << (in ? static_cast<const char *>(in) : "unknown") << std::endl;
    ws_session->wsi = NULL;
    ws_session->state = WsSession::Closed;
    break;

  case LWS_CALLBACK_CLIENT_ESTABLISHED:
    std::cout << "websocket connected" << std::endl;
    ws_session->state = WsSession::Open;
    break;

  case LWS_CALLBACK_CLIENT_CLOSED:
    std::cout << "websocket closed" << std::endl;
    ws_session->wsi = NULL;
    ws_session->state = WsSession::Closed;
    break;

  case LWS_CALLBACK_EVENT_WAIT_CANCELLED:
    // Woken up by lws_cancel_service() from the depth listener thread.
    if (ws_session && ws_session->state == WsSession::Open && ws_session->wsi)
    {
      std::lock_guard<std::mutex> lock(ws_session->mutex);
      if (ws_session->has_pending)
        lws_callback_on_writable(ws_session->wsi);
    }
    break;

  case LWS_CALLBACK_CLIENT_WRITEABLE:
  {
    if (ws_session->state == WsSession::Closing)
    {
      lws_close_reason(wsi, LWS_CLOSE_STATUS_NORMAL, NULL, 0);
      return -1;
    }

    {
      std::lock_guard<std::mutex> lock(ws_session->mutex);
      if (!ws_session->has_pending)
        break;
      ws_session->pending.swap(ws_session->sending);
      ws_session->has_pending = false;
    }

    const size_t size = ws_session->sending.size() - LWS_PRE;
    int n = lws_write(wsi, &ws_session->sending[LWS_PRE], size, LWS_WRITE_BINARY);
    if (n < (int)size)
    {
      std::cerr << "websocket write error" << std::endl;
      return -1;
    }
    ws_session->frames_sent++;
    if (ws_session->frames_sent % 100 == 0)
      std::cout << "Sent " << ws_session->frames_sent << " depth frames. Ctrl-C to stop." << std::endl;
    break;
  }

  default:
    break;
  }

  return lws_callback_http_dummy(wsi, reason, user, in, len);
}

// Periodic wake-up so the service loop notices SIGINT and frame timeouts even when idle.
static void ws_tick(lws_sorted_usec_list_t *sul)
{
  lws_sul_schedule(ws_session->context, 0, sul, ws_tick, 100 * LWS_US_PER_MS);
}

/** Copies each depth frame and asks the websocket service loop to send it. */
class DepthWsListener : public libfreenect2::FrameListener
{
public:
  DepthWsListener(WsSession &session) : session_(session), frames_received_(0) {}

  virtual bool onNewFrame(libfreenect2::Frame::Type type, libfreenect2::Frame *frame)
  {
    if (type != libfreenect2::Frame::Depth)
      return false;

    const size_t size = frame->width * frame->height * frame->bytes_per_pixel;
    {
      std::lock_guard<std::mutex> lock(session_.mutex);
      // An unsent older frame is replaced: only the latest frame matters.
      session_.pending.resize(LWS_PRE + size);
      std::memcpy(&session_.pending[LWS_PRE], frame->data, size);
      session_.has_pending = true;
    }
    frames_received_++;
    lws_cancel_service(session_.context);
    return false;
  }

  size_t framesReceived() const { return frames_received_; }

private:
  WsSession &session_;
  std::atomic<size_t> frames_received_;
};

/** Parses "[ws://]host[:port][/path]". */
static bool parseEndpoint(const std::string &endpoint, std::string &host, int &port, std::string &path)
{
  std::string rest = endpoint;
  const std::string scheme = "ws://";
  if (rest.compare(0, scheme.size(), scheme) == 0)
    rest = rest.substr(scheme.size());

  size_t slash = rest.find('/');
  path = (slash == std::string::npos) ? "/" : rest.substr(slash);
  std::string hostport = rest.substr(0, slash);

  size_t colon = hostport.rfind(':');
  port = DEFAULT_WS_PORT;
  if (colon != std::string::npos)
  {
    const std::string portstr = hostport.substr(colon + 1);
    char *end = NULL;
    long value = strtol(portstr.c_str(), &end, 10);
    if (portstr.empty() || *end != '\0' || value < 1 || value > 65535)
      return false;
    port = (int)value;
    hostport = hostport.substr(0, colon);
  }
  host = hostport;
  return !host.empty();
}

/** Asks for a normal websocket close and waits briefly for the closing handshake. */
static void closeWebSocket(WsSession &session)
{
  if (session.state == WsSession::Open && session.wsi)
  {
    session.state = WsSession::Closing;
    lws_callback_on_writable(session.wsi);
    std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (session.state != WsSession::Closed && std::chrono::steady_clock::now() < deadline)
    {
      if (lws_service(session.context, 0) < 0)
        break;
    }
  }
  lws_context_destroy(session.context);
  session.context = NULL;
  session.wsi = NULL;
  session.state = WsSession::Closed;
}

/**
 * Main application entry point.
 *
 * Accepted argumemnts:
 * - cpu Perform depth processing with the CPU.
 * - gl  Perform depth processing with OpenGL.
 * - cl  Perform depth processing with OpenCL.
 * - <number> Serial number of the device to open.
 * - -ws=[ws://]host[:port][/path] Websocket endpoint (default localhost:3001).
 */
int main(int argc, char *argv[])
{
  std::string program_path(argv[0]);
  std::cerr << "Version: " << LIBFREENECT2_VERSION << std::endl;
  std::cerr << "Usage: " << program_path << " [-ws=[ws://]<host>[:<port>][/<path>]] [-gpu=<id>] [gl | cl | clkde | cuda | cudakde | cpu] [<device serial>]" << std::endl;
  std::cerr << "        [-help] [-version] [-frames <number of frames to send>]" << std::endl;
  std::cerr << "Default websocket endpoint: " << DEFAULT_WS_HOST << ":" << DEFAULT_WS_PORT << std::endl;
  std::cerr << "Each depth frame is sent as one binary message: 512x424 float32 (millimeters), row-major, host byte order." << std::endl;

#if defined(WIN32) || defined(_WIN32) || defined(__WIN32__)
  libfreenect2::setGlobalLogger(libfreenect2::createConsoleLogger(libfreenect2::Logger::Info));
#else
  libfreenect2::setGlobalLogger(libfreenect2::createConsoleLogger(libfreenect2::Logger::Debug));
#endif

  libfreenect2::Freenect2 freenect2;
  libfreenect2::Freenect2Device *dev = 0;
  libfreenect2::PacketPipeline *pipeline = 0;

  std::string serial = "";
  std::string ws_host = DEFAULT_WS_HOST;
  int ws_port = DEFAULT_WS_PORT;
  std::string ws_path = "/";

  int deviceId = -1;
  size_t framemax = -1;

  for(int argI = 1; argI < argc; ++argI)
  {
    const std::string arg(argv[argI]);

    if(arg == "-help" || arg == "--help" || arg == "-h" || arg == "-v" || arg == "--version" || arg == "-version")
    {
      // Just let the initial lines display at the beginning of main
      delete pipeline;
      return 0;
    }
    else if(arg.find("-ws=") == 0)
    {
      if (!parseEndpoint(arg.substr(4), ws_host, ws_port, ws_path))
      {
        std::cerr << "invalid websocket endpoint '" << arg.substr(4) << "'" << std::endl;
        delete pipeline;
        return -1;
      }
    }
    else if(arg.find("-gpu=") == 0)
    {
      if (pipeline)
      {
        std::cerr << "-gpu must be specified before pipeline argument" << std::endl;
        delete pipeline;
        return -1;
      }
      deviceId = atoi(argv[argI] + 5);
    }
    else if(arg == "cpu")
    {
      if(!pipeline)
        pipeline = new libfreenect2::CpuPacketPipeline();
    }
    else if(arg == "gl")
    {
#ifdef LIBFREENECT2_WITH_OPENGL_SUPPORT
      if(!pipeline)
        pipeline = new libfreenect2::OpenGLPacketPipeline();
#else
      std::cout << "OpenGL pipeline is not supported!" << std::endl;
#endif
    }
    else if(arg == "cl")
    {
#ifdef LIBFREENECT2_WITH_OPENCL_SUPPORT
      if(!pipeline)
        pipeline = new libfreenect2::OpenCLPacketPipeline(deviceId);
#else
      std::cout << "OpenCL pipeline is not supported!" << std::endl;
#endif
    }
    else if(arg == "clkde")
    {
#ifdef LIBFREENECT2_WITH_OPENCL_SUPPORT
      if(!pipeline)
        pipeline = new libfreenect2::OpenCLKdePacketPipeline(deviceId);
#else
      std::cout << "OpenCL pipeline is not supported!" << std::endl;
#endif
    }
    else if(arg == "cuda")
    {
#ifdef LIBFREENECT2_WITH_CUDA_SUPPORT
      if(!pipeline)
        pipeline = new libfreenect2::CudaPacketPipeline(deviceId);
#else
      std::cout << "CUDA pipeline is not supported!" << std::endl;
#endif
    }
    else if(arg == "cudakde")
    {
#ifdef LIBFREENECT2_WITH_CUDA_SUPPORT
      if(!pipeline)
        pipeline = new libfreenect2::CudaKdePacketPipeline(deviceId);
#else
      std::cout << "CUDA pipeline is not supported!" << std::endl;
#endif
    }
    else if(arg.find_first_not_of("0123456789") == std::string::npos) //check if parameter could be a serial number
    {
      serial = arg;
    }
    else if(arg == "-frames")
    {
      ++argI;
      framemax = argI < argc ? strtol(argv[argI], NULL, 0) : 0;
      if (framemax == 0) {
        std::cerr << "invalid frame count '" << (argI < argc ? argv[argI] : "") << "'" << std::endl;
        delete pipeline;
        return -1;
      }
    }
    else
    {
      std::cout << "Unknown argument: " << arg << std::endl;
    }
  }

  signal(SIGINT, sigint_handler);
  signal(SIGTERM, sigint_handler);

  // Open the websocket first: nothing else is started if it cannot be opened.
  WsSession session;
  ws_session = &session;

  lws_set_log_level(LLL_ERR | LLL_WARN, NULL);

  static const lws_protocols protocols[] = {
    { "depthws", ws_callback, 0, 0, 0, NULL, 0 },
    { NULL, NULL, 0, 0, 0, NULL, 0 }
  };

  lws_context_creation_info info;
  std::memset(&info, 0, sizeof(info));
  info.port = CONTEXT_PORT_NO_LISTEN;
  info.protocols = protocols;
  info.gid = -1;
  info.uid = -1;

  session.context = lws_create_context(&info);
  if (!session.context)
  {
    std::cerr << "failed to create websocket context!" << std::endl;
    delete pipeline;
    return -1;
  }
  lws_sul_schedule(session.context, 0, &session.tick, ws_tick, 100 * LWS_US_PER_MS);

  std::string ws_host_header = ws_host + ":" + std::to_string(ws_port);
  lws_client_connect_info ccinfo;
  std::memset(&ccinfo, 0, sizeof(ccinfo));
  ccinfo.context = session.context;
  ccinfo.address = ws_host.c_str();
  ccinfo.port = ws_port;
  ccinfo.path = ws_path.c_str();
  ccinfo.host = ws_host_header.c_str();
  ccinfo.origin = ws_host_header.c_str();
  ccinfo.pwsi = &session.wsi;

  std::cout << "connecting to ws://" << ws_host_header << ws_path << std::endl;
  if (!lws_client_connect_via_info(&ccinfo))
    session.state = WsSession::Closed;

  while (session.state == WsSession::Connecting && !depthws_shutdown)
  {
    if (lws_service(session.context, 0) < 0)
      break;
  }

  if (session.state != WsSession::Open)
  {
    std::cout << "failure opening websocket!" << std::endl;
    closeWebSocket(session);
    delete pipeline;
    return -1;
  }

  if(freenect2.enumerateDevices() == 0)
  {
    std::cout << "no device connected!" << std::endl;
    closeWebSocket(session);
    delete pipeline;
    return -1;
  }

  if (serial == "")
  {
    serial = freenect2.getDefaultDeviceSerialNumber();
  }

  if(pipeline)
  {
    // openDevice() takes ownership of the pipeline, even on failure.
    dev = freenect2.openDevice(serial, pipeline);
  }
  else
  {
    dev = freenect2.openDevice(serial);
  }

  if(dev == 0)
  {
    std::cout << "failure opening device!" << std::endl;
    closeWebSocket(session);
    return -1;
  }

  DepthWsListener listener(session);
  dev->setIrAndDepthFrameListener(&listener);

  if (!dev->startStreams(false, true))
  {
    std::cout << "failure starting depth stream!" << std::endl;
    dev->close();
    closeWebSocket(session);
    return -1;
  }

  std::cout << "device serial: " << dev->getSerialNumber() << std::endl;
  std::cout << "device firmware: " << dev->getFirmwareVersion() << std::endl;

  int exit_code = 0;
  size_t last_frames_received = 0;
  std::chrono::steady_clock::time_point last_frame_time = std::chrono::steady_clock::now();

  while(!depthws_shutdown && session.state == WsSession::Open
        && (framemax == (size_t)-1 || session.frames_sent < framemax))
  {
    if (lws_service(session.context, 0) < 0)
    {
      std::cerr << "websocket service error!" << std::endl;
      exit_code = -1;
      break;
    }

    std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
    size_t received = listener.framesReceived();
    if (received != last_frames_received)
    {
      last_frames_received = received;
      last_frame_time = now;
    }
    else if (now - last_frame_time > std::chrono::seconds(10))
    {
      std::cout << "timeout!" << std::endl;
      exit_code = -1;
      break;
    }
  }

  if (session.state == WsSession::Closed)
  {
    std::cout << "websocket closed, stopping." << std::endl;
    exit_code = -1;
  }

  dev->stop();
  dev->close();

  closeWebSocket(session);
  ws_session = NULL;

  return exit_code;
}
