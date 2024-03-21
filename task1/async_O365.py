import asyncio
import time

from O365 import Account


def send_email(*args):
  """

  :param args: [0] client
               [1] secret
               [2] mail box
               [3] mail box password
               [4] recipient address
  :return:

  """
  # credentials = (args[0], args[1])
  #
  # account = Account(credentials)
  #
  # if account.authenticate(scopes=['basic', 'message_all']):
  #   print('Authenticated!')
  # mailbox = account.mailbox()
  # message = account.mailbox()
  # message.subject = "task2 subject"
  # message.body = 'task2 notification body'
  # message.to.add(args[4])
  # await message.send()

  print(f"message sent to receiver: {args[0]}")








