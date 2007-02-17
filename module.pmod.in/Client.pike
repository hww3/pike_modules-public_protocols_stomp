//! this is a Stomp client.

inherit .protocol;

string broker_url;
static Stdio.File conn;
static string user = "";
static string pass = "";

static string session;
static int is_running = 0;

private int backoff_value = 1;
private int reconnect = 0;

static mapping ack = ([]);
mapping subscribers = ([]);
ADT.Queue pending_messages = ADT.Queue();

void set_background()
{
  conn->set_read_callback(streaming_decode);
  conn->set_close_callback(stomp_close_callback);
  conn->set_nonblocking_keep_callbacks();
}

//!
void create(string _broker_url, int(0..1)|void _reconnect)
{
  frame_handler = client_frame_handler;  
  
  broker_url = _broker_url;
  reconnect = _reconnect;

  connect(broker_url);
}

//!
static void set_auth(string u, string p)
{
  user = u;
  pass = p;
}

//!
string get_session()
{
  return session;
}

//!
static void connect(string broker_url)
{
  Standards.URI url;
  string host;
  int port;
  Stdio.File c = Stdio.File();

  url = Standards.URI(broker_url);
 

  if(url->scheme != "stomp")
  {
    throw(Error.Generic("expected a stomp url; got " + url->scheme + ".\n"));
  }  

  host = url->host;
  port = url->port || 61613;

  if(url->user)
    set_auth(url->user, url->password);

  if(!c->connect(host, port))
    throw(Error.Generic("Public.Protocols.Stomp.Client: unable to connect.\n"));

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
  is_running = 1;

  //werror("client running.\n");

  return;
}

//!  begin a transaction.
//!  @returns
//!   the transaction identifier string
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


//! commit a transaction
//!  @param txid
//!     the transaction identifier of the transaction to commit.
//!  @param receipt
//!     should we await confirmation of this command from the server?
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

//! abort a transaction, rolling back any messages
//! @param txid
//!  the transaction identifier of the transaction to abort.
//!  @param receipt
//!     should we await confirmation of this command from the server?
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

//! subscribe to a topic or queue
//!  @param callback
//!   a function that takes receives a Frame object for each message 
//!   delivered and returns one to acknowledge or zero to refuse receipt 
//!   of the message
//!  @param acknowledge
//!    should we require recieved messages to be acknowledged?
//!  @param receipt
//!     should we await confirmation of this command from the server?
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

  subscribers[destination] = ({callback, acknowledge, receipt});

  return 1;
}

//!  unsubscribe from a topic or queue.
//!  @param destination
//!     the queue or topic we wish to unsubscribe from.
//!  @param receipt
//!     should we await confirmation of this command from the server?
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

  m_delete(subscribers, destination);

  return 1;
}

//! send a message to a queue or topic
//!
//! @param destination
//!     the name of the topic or queue to send the message to.
//! @param message
//!     the contents of the message to be sent
//! @param headers
//!     a list of headers we wish to include in the message
//! @param txid
//!     transaction identifier of the transaction we wish to associate
//!     this message with.
//!  @param receipt
//!     should we await confirmation of this command from the server?
int send(string destination, string message, mapping|void headers, string|void txid, int(0..1)|void receipt)
{
  string messageid;

  // we just can't accomodate this type of request.
  if(!is_running && receipt)
  {
     throw(Error.Generic("Unable to send message with receipt while broker is unavailable.\n"));
  }

  Frame f = Frame();

  if(headers) f->set_headers(headers);

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
  {
    if(!is_running)
      delayed_send_frame(f);
    else
      send_frame(f);
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

void stomp_close_callback(mixed id)
{
  werror("server closed the connection!\n");
  is_running = 0;
  if(reconnect)
  {
    call_out(reconnect_callout, backoff_value);
  }
}

void reconnect_callout()
{
   if(catch(connect(broker_url)))
   {
      backoff_value += random(backoff_value+1);
      werror("connection failed, retrying in %d seconds.\n", backoff_value);
      call_out(reconnect_callout, backoff_value);
   }
   else
   {
      backoff_value = 1;
      is_running = 1;
      call_out(process_pending, 1);
      call_out(resubscribe_client, 1);
   }
}

void resubscribe_client()
{
   foreach(subscribers;string dest; array d)
   {
     subscribe(dest, d[0], d[1], d[2]);
   }
}

static void process_pending()
{
   werror("process_pending()\n");
   while(!pending_messages->is_empty())
   {
     send_frame(pending_messages->read());
   }
}

static void delayed_send_frame(Frame f)
{
  pending_messages->write(f);
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
      int r = subscribers[dest][0](f);
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
