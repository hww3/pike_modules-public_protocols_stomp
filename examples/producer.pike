object s;

int main(int argc, array(string) argv)
{

  if(argc<4) 
  {
    werror("usage: %s stompurl destination message\n", argv[0]);
    return 1;
  }

  s = Public.Protocols.Stomp.Client(argv[1]);
  werror("connected.\n");

  int r = s->send(argv[2], argv[3]);

  werror("sent.\n");
  return r;
}
