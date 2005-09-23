//! this is a Stomp client.

inherit .protocol;

static Stdio.File conn;

static string user="";
static string pass="";

void create()
{
  
}

void set_auth(string u, string p)
{
  user = u;
  pass = p;
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

  write(f->get_command() + "\n");
  write(f->get_header("session") + "\n");

  return;
}

static void send_frame(Frame f)
{
  conn->write((string)f);
}

static Frame receive_frame()
{
  string d = conn->read(10000, 1);

  if(!d) error("no data received!\n");

  Frame f = decode_frame(d);

  return f;
}
