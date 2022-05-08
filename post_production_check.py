import socket

ip_list = [['0.0.0.0', 8001], ['0.0.0.0', 8002]]
for ip in ip_list:
  a_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

  location = (ip[0], ip[1])
  result_of_check = a_socket.connect_ex(location)

  if result_of_check == 0:
    print("Website with ip: {} {} is up and running.".format(ip[0], ip[1]))
  else:
    raise Exception("Website with ip: {} {} failed to connect.".format(ip[0], ip[1]))