//! this is a Stomp client.

inherit .protocol;

static Stdio.File conn;

static string user = "";
static string pass = "";

static string session;

static mapping ack = ([]);
mapping subscribers = ([]);

void set_background()
{
  conn->set_read_callback(streaming_decode);
  conn->set_nonblocking_keep_callbacks();
}


void create()
{
  frame_handler = client_frame_handler;  
}

void set_auth(string u, string p)
{
  user = u;
  pass = p;
}

string get_session()
{
  return session;
}

void connect(string host, int port)
{
  Stdio.File c = Stdio.File();

  c->connect(host, port);
//    error("Stomp.Client: unable to connect.\n");

  conn = c;

  Frame f = Frame();
 
  f->set_command("CONNECT");
  if(user)
  {
    f->set_header("login", user);
    f->set_header("passcode", pass);
  }

  f = send_frame_get_response(f);

  if(f->get_command() != "CONNECTED")
    error("Unexpected response from server, got %s\n", f->get_command());

  session = f->get_header("session");

  if(!session) 
    error("Missing session id from response.\n");  

  set_background();

  //werror("client running.\n");

  return;
}

string begin(int(0..1)|void receipt)
{
  string messageid;
  Frame f = Frame();

  f->set_command("BEGIN");

  if(receipt)
  { 
    messageid = "message-" + Standards.UUID.new_string();
    f->set_header("receipt", messageid);
    f = send_frame_get_response(f);
    if(f->get_command() != "RECEIPT")
      error("out of sync response, expected RECEIPT, got %s\n", f->get_command());
    if(f->get_header("receipt-id") != messageid)
      error("incorrect receipt id received.\n");
  }
  else 
    send_frame(f);

  return messageid;
}


int commit(string txid, int(0..1)|void receipt)
{
  string messageid;
  Frame f = Frame();
  f->set_header("transaction", txid);
  f->set_command("COMMIT");  

  if(receipt)
  {
    messageid = "message-" + random(10000000);
    f->set_header("receipt", messageid);

    f = send_frame_get_response(f);
    if(f->get_command() != "RECEIPT")
      error("out of sync response, expected RECEIPT, got %s\n", f->get_command());
    if(f->get_header("receipt-id") != messageid)
      error("incorrect receipt id received.\n");
  }
  else
    send_frame(f);

  return 1;
}

int abort(string txid, int(0..1)|void receipt)
{
  string messageid;
  Frame f = Frame();

  f->set_header("transaction", txid);

  f->set_command("ABORT");
  
  if(receipt)
  {
    messageid = "message-" + random(10000000);
    f->set_header("receipt", messageid);
    f = send_frame_get_response(f);
    if(f->get_command() != "RECEIPT")
      error("out of sync response, expected RECEIPT, got %s\n", f->get_command());
    if(f->get_header("receipt-id") != messageid)
      error("incorrect receipt id received.\n");
  }
  else
    send_frame(f);

  return 1;

}

int subscribe(string destination, function callback, int(0..1)|void acknowledge, int(0..1)|void receipt)
{
  string messageid;
  Frame f = Frame();

  if(acknowledge)
  {
    f->set_header("ack", "client");
    ack[destination] = 1;
  }
  else
    f->set_header("ack", "auto");

  f->set_command("SUBSCRIBE");

  f->set_header("destination", destination);


  if(receipt)
  {
    messageid = "message-" + random(10000000);
    f->set_header("receipt", messageid);
    f = send_frame_get_response(f);
    if(f->get_command() != "RECEIPT")
      error("out of sync response, expected RECEIPT, got %s\n", f->get_command());
    if(f->get_header("receipt-id") != messageid)
      error("incorrect receipt id received.\n");
  }
  else
    send_frame(f);

  subscribers[destination] = callback;

  return 1;
}

int unsubscribe(string destination, int(0..1)|void receipt)
{
  string messageid;
  Frame f = Frame();

  f->set_command("UNSUBSCRIBE");

  f->set_header("destination", destination);
  
  if(receipt)
  {
    messageid = "message-" + random(10000000);
    f->set_header("receipt", messageid);
    f = send_frame_get_response(f);
    if(f->get_command() != "RECEIPT")
      error("out of sync response, expected RECEIPT, got %s\n", f->get_command());
    if(f->get_header("receipt-id") != messageid)
      error("incorrect receipt id received.\n");
  }
  else
    send_frame(f);

  return 1;
}

int send(string destination, string message, string|void txid, int(0..1)|void receipt)
{
  string messageid;
  Frame f = Frame();

  if(txid)
    f->set_header("transaction", txid);

  f->set_command("SEND");

  f->set_header("destination", destination);

  f->set_body(message);
  
  if(receipt)
  {
    messageid = "message-" + random(10000000);
    f->set_header("receipt", messageid);
    f = send_frame_get_response(f);
    if(f->get_command() != "RECEIPT")
      error("out of sync response, expected RECEIPT, got %s\n", f->get_command());
    if(f->get_header("receipt-id") != messageid)
      error("incorrect receipt id received.\n");
  }
  else
    send_frame(f);

  return 1;
}

Frame await_message()
{
  return receive_frame();
}

static Frame receive_frame()
{
  string d;
  Frame f;
  conn->set_blocking_keep_callbacks();
  d = (sizeof(buffer)?buffer: conn->read(10000, 1));
  conn->set_nonblocking_keep_callbacks();

  // werror("got data: %O\n", d);

  if(!d) error("no data received!\n");

  [f, buffer] = decode_frame(d);

// werror("decoded frame.\n");
  return f;
}

static Frame send_frame_get_response(Frame f)
{
  send_frame(f, 1);
  return receive_frame();
  
}

static void send_frame(Frame f, int|void block)
{
  conn->set_blocking_keep_callbacks();
  conn->write((string)f);
// werror("wrote frame.\n");
  if(!block)
    conn->set_nonblocking_keep_callbacks();
}

void client_frame_handler(Frame f)
{
  // werror("got frame: %O %O\n", f->get_headers(), f->get_body());

  if(f->get_command() == "MESSAGE")
  {
    string dest = f->get_header("destination");

    if(subscribers[dest])
    {
      int r = subscribers[dest](f);
      if(ack[dest] && r)
      {
        Frame fr = Frame();
        fr->set_command("ACK");
        fr->set_header("message-id", f->get_header("message-id"));
        if(f->get_header("transaction"))
          fr->set_header("transaction", f->get_header("transaction"));
        send_frame(fr);
      }
    }
  }
  

}
