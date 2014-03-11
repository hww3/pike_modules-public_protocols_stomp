object s;

int main(int argc, array(string) argv)
{
  s = Public.Protocols.Stomp.Client();

  if(argc<4) 
  {
    werror("usage: %s servername portnumber destination\n", argv[0]);
    return 1;
  }

  s->connect(argv[1], (int)argv[2]);

  s->subscribe(argv[3], 
      lambda(Public.Protocols.Stomp.protocol.Frame f){
           werror("got a message: %O\n", f->get_body()); return 1;}
  );

  // enable nonblocking callback mode.
  s->set_background();


  return -1;
}
