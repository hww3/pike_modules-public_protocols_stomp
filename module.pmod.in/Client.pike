//! this is a Stomp client.

inherit .protocol;

static Stdio.File conn;

static string user = "";
static string pass = "";

static string session;

static string buffer="";

void create()
{
  
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
  if(!c->connect(host, port))
    error("Stomp.Client: unable to connect.\n");

  conn = c;

  Frame f = Frame();
 
  f->set_command("CONNECT");
  if(user)
  {
    f->set_header("login", user);
    f->set_header("passcode", pass);
  }

  send_frame(f);
  f = receive_frame();

  if(f->get_command() != "CONNECTED")
    error("Unexpected response from server, got %s\n", f->get_command());

  session = f->get_header("session");

  if(!session) 
    error("Missing session id from response.\n");

  return;
}

int begin(int(0..1)|void receipt)
{
  string messageid;
  Frame f = Frame();

  f->set_command("BEGIN");

  if(receipt)
  {
    messageid = "message-" + random(10000000);
    f->set_header("receipt", messageid);
  }

  send_frame(f);
  
  if(receipt)
  {
    f = receive_frame();
    if(f->get_command() != "RECEIPT")
      error("out of sync response, expected RECEIPT, got %s\n", f->get_command());
    if(f->get_header("receipt-id") != messageid)
      error("incorrect receipt id received.\n");

    return 1;
  }

  return 1;
}

int commit(int(0..1)|void receipt)
{
  string messageid;
  Frame f = Frame();

  f->set_command("COMMIT");

  if(receipt)
  {
    messageid = "message-" + random(10000000);
    f->set_header("receipt", messageid);
  }

  send_frame(f);
  
  if(receipt)
  {
    f = receive_frame();
    if(f->get_command() != "RECEIPT")
      error("out of sync response, expected RECEIPT, got %s\n", f->get_command());
    if(f->get_header("receipt-id") != messageid)
      error("incorrect receipt id received.\n");

    return 1;
  }

  return 1;
}

int abort(int(0..1)|void receipt)
{
  string messageid;
  Frame f = Frame();

  f->set_command("ABORT");

  if(receipt)
  {
    messageid = "message-" + random(10000000);
    f->set_header("receipt", messageid);
  }

  send_frame(f);
  
  if(receipt)
  {
    f = receive_frame();
    if(f->get_command() != "RECEIPT")
      error("out of sync response, expected RECEIPT, got %s\n", f->get_command());
    if(f->get_header("receipt-id") != messageid)
      error("incorrect receipt id received.\n");

    return 1;
  }

  return 1;

}

int subscribe(string destination, int(0..1)|void receipt)
{
  string messageid;
  Frame f = Frame();

  f->set_command("SUBSCRIBE");

  f->set_header("destination", destination);

  if(receipt)
  {
    messageid = "message-" + random(10000000);
    f->set_header("receipt", messageid);
  }

  send_frame(f);
  
  if(receipt)
  {
    f = receive_frame();
    if(f->get_command() != "RECEIPT")
      error("out of sync response, expected RECEIPT, got %s\n", f->get_command());
    if(f->get_header("receipt-id") != messageid)
      error("incorrect receipt id received.\n");

    return 1;
  }

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
  }

  send_frame(f);
  
  if(receipt)
  {
    f = receive_frame();
    if(f->get_command() != "RECEIPT")
      error("out of sync response, expected RECEIPT, got %s\n", f->get_command());
    if(f->get_header("receipt-id") != messageid)
      error("incorrect receipt id received.\n");

    return 1;
  }

  return 1;

}

int send(string destination, string message, int(0..1)|void receipt)
{
  string messageid;
  Frame f = Frame();

  f->set_command("SEND");

  f->set_header("destination", destination);

  if(receipt)
  {
    messageid = "message-" + random(10000000);
    f->set_header("receipt", messageid);
  }

  f->set_body(message);

  send_frame(f);
  
  if(receipt)
  {
    f = receive_frame();
    if(f->get_command() != "RECEIPT")
      error("out of sync response, expected RECEIPT, got %s\n", f->get_command());
    if(f->get_header("receipt-id") != messageid)
      error("incorrect receipt id received.\n");

    return 1;
  }

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
  //werror("buffer: %O\n", buffer);
  d = (sizeof(buffer)?buffer: conn->read(10000, 1));

  if(!d) error("no data received!\n");

  [f, buffer] = decode_frame(d);

  return f;
}

static void send_frame(Frame f)
{
  conn->write((string)f);
}


