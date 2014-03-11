object s;

int main(int argc, array(string) argv)
{

  if(argc<3) 
  {
    werror("usage: %s stompurl destination\n", argv[0]);
    return 1;
  }

  s = Public.Protocols.Stomp.Client(argv[1]);
  werror("connected.\n");

  s->subscribe(argv[2], 
      lambda(Public.Protocols.Stomp.protocol.Frame f){
           werror("got a message: %O\n", f->get_body()); return 1;}
  );
  werror("subscribed\n");

  // enable nonblocking callback mode.
  s->set_background();

  return -1;
}
