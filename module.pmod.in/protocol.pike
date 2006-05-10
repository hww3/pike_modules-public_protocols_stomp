//! this is the low level protocol handler for Stomp.
//! for more information, see the stomp hompage
//! http://stomp.codehaus.org

constant STREAM_WAITING_FRAME = 0;
constant STREAM_HAVE_COMMAND = 1;
constant STREAM_HAVE_HEADERS = 2;
constant STREAM_HAVE_BODY = 3;

string buffer="";
int streaming_decode_state = STREAM_WAITING_FRAME;
function frame_handler;

mapping streaming_decode_storage = ([]);

void streaming_decode(int handle, string data)
{
  Frame frame;

  if(!data) return;

  //werror("got data: %O\n", data);

  buffer += data;

  do
  { 
    frame = low_streaming_decode();
    if(frame && frame_handler)
      frame_handler(frame);
  }
  while(frame);
}

Frame low_streaming_decode()
{
  int stop;

  do
  {
    //werror("buffer: %O \n", buffer);
    if(!buffer || !strlen(buffer))
      stop = 1;

    int num;
    switch(streaming_decode_state)
    {
      case 0: // STREAM_WAITING_FRAME
        // we first need to get the command.
        num =  sscanf(buffer, "%s\n%s", streaming_decode_storage->command, buffer);
        if(num == 0)
          stop = 1;
        else streaming_decode_state = STREAM_HAVE_COMMAND;
        break;
      case 1: // STREAM_HAVE_COMMAND
        // ok, we need to get the whole header block before we do anything.
        num =  sscanf(buffer, "%s\n\n%s", streaming_decode_storage->headerblock, buffer);
        if(num == 0)
          stop = 1;
        else
        {
          streaming_decode_storage->headers = ([]);
          string h,v;

          foreach(streaming_decode_storage->headerblock / "\n";; string header)
          {
            //werror("decode header " + header + "\n");
            int l = search(header, ":");
            if(l > -1)
            { 
              h = String.trim_whites(header[0..(l-1)]);
              v = String.trim_whites(header[(l+1)..]);
            }
            else error("Stom.protocol: invalid header received.\n");

            streaming_decode_storage->headers[h] = v;
          }
          streaming_decode_state = STREAM_HAVE_HEADERS;
        }
        break;
      case 2: // STREAM_HAVE_HEADERS
        // ok, now we should be looking for the body.
        // if we have a content-length header, we look for that much data.
        string searchlen = "";

        // is it safe to assume that content-length is always an integer?
        if(streaming_decode_storage->headers["content-length"])
          searchlen = streaming_decode_storage->headers["content-length"];

        num =  sscanf(buffer, "%s" + searchlen + "\000%s", streaming_decode_storage->body, buffer);
        if(num == 0)
          stop = 1;
        else
        {
          if(has_prefix(buffer, "\n"))
            buffer = buffer[1..];
          streaming_decode_state = STREAM_HAVE_BODY;
        }
        break;
      case 3: // STREAM_HAVE_BODY
        //werror("have frame body: %O\n", streaming_decode_storage->body);
        Frame f = Frame();
        f->set_command(streaming_decode_storage->command);
        f->set_body(streaming_decode_storage->body);
        f->set_headers(streaming_decode_storage->headers);
        streaming_decode_state = STREAM_WAITING_FRAME;
        return f;
        break;
      default:
        error("Invalid decode state!\n");
    }
  }  while(!stop);

  return 0;
}


array(Frame|string) decode_frame(string data)
{
  string command ="";
  mapping headers = ([]);
  string body, rest = "";
  Frame f;
  array z;
  
  data = utf8_to_string(data);
//werror("data: %O\n\n", data);

  if(catch(z = array_sscanf(data, "%s\n%s\000%s")))
    error("Error decoding Frame, invalid data format.\n");

  if(sizeof(z) == 3)
  {
    command = z[0];
    body = z[1];
    rest = z[2];
  }

  else if(sizeof(z) ==2)
  {
    command = z[0];
    body = z[1];
  }

  else
  {
    command = z[0];
  }

  f = Frame();

  f->set_command(command);

  int still_looking = 1;
 
  do{
    // if we have an empty line, then that means we're out of headers.
    if(body[0..0] == "\n") 
    {
      still_looking = 0;
      if(sizeof(body) > 1)
        body = body[1..];
      else body = "";
    }

    else
    {
      string h, v;

      if(catch([h, v, body] = array_sscanf(body, "%s:%s\n%s")))
        error("Unable to decode frame, bogus line.\n");

      f->set_header(h, v);
    }

  } while(still_looking == 1);  

  if(body)
    f->set_body(body);

  if(rest[0..0] == "\n")
    rest = rest[1..];

  return ({f, rest});
}


class Frame
{
  static string command = "";
  static mapping headers = ([]);
  static string body = "";

  void create()
  {
  }

  void set_command(string c)
  {
    command = c;
  }
   
  string get_command()
  {
    return command;
  }

  void set_body(string b)
  {
    body = b;
  }

  string get_body()
  {
    return body;
  }

  mapping get_headers()
  {
    return headers + ([]);
  }

  string get_header(string h)
  {
    return headers[h];
  }

  void set_header(string h, string v)
  {
    headers[h] = v;
  }

  void set_headers(mapping h)
  {
    headers = h;
  }

  mixed cast(string type)
  {
//werror("frame: %O\n\n", render_frame());
    if(type == "string")
      return render_frame();
    else
      error("Unknown cast type %s\n", type);

  }

  string render_frame()
  {
    string f = "";

    f += command + "\n";

    if(body) headers["content-length"] = (string)(sizeof(body));

// werror("render_frame(): %O %O\n", headers, body);

    foreach(headers; string h; string v)
      f += string_to_utf8(h) + ":" + string_to_utf8(v) + "\n";


    f += "\n";

    f += string_to_utf8(body);

    f += "\000";


    return f;
  }  

}
