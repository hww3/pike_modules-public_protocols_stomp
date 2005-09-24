//! this is the low level protocol handler for Stomp.
//! for more information, see the stomp hompage
//! http://stomp.codehaus.org


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

  string get_header(string h)
  {
    return headers[h];
  }

  void set_header(string h, string v)
  {
    headers[h] = v;
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
    foreach(headers; string h; string v)
      f += string_to_utf8(h) + ":" + string_to_utf8(v) + "\n";

    f += "\n";

    f += string_to_utf8(body);

    f += "\000";


    return f;
  }

  

}
