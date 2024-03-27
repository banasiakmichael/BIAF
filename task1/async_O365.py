import asyncio
import aiofiles
import time
from O365 import Account


mess_1 = """
The following project(s) have had employees listed below who have booked 30hrs or more to your project(s) in the period between December & February who have received a salary change in February.
 Note, there may be other employees with less than 30hrs hours booked in the 3month period also affected.
  Salary changes may or may not have any impact on the hourly cost rates (e.g. contract working hours changes would result in a salary change but not necessarily a hour cost rate change).
"""

mess_2 = """
This email is to notify you to allow you to investigate at project level and if appropriate, if nothing else,
 please factor this into your Direct Cost ETC calculations. Please also consider your contract and whether the renumeration mechanism (e.g. bill rates) is affected by the change.
"""


async def send_email(*args):
  """

  :param args: [0] client
               [1] secret
               [2] mail box
               [3] mail box password
               [4] PM emial address
               [5] data frame: Project Number, Employee nr, Employee name, Project Manager email address
  :return:

  """
  filt_ = (args[5]['Project Manager'] == args[4])
  df = args[5].loc[filt_] if not None else None

  if df is not None:
    df = df.drop('Project Manager', axis=1)
    scope = df.to_html(index=False)

    message_ = f"""
        <html><body>
        <br>
          Dear {args[4].split('.')[0]},
        <br><br>
          {mess_1}
        <br><br>
          {mess_2}
        <br><br>
          {scope}
        <br><br>
          This is an automated email, please do not reply directly as responses are not monitored.
        <br><br>
          Many Thanks,
        <br><br>
          The Europe BI & Analytics team.
        <br><br><br>
        </body></hmtl>"""

    async with aiofiles.open('pm.html', mode='a') as f:
      await f.write(f"{message_}" + '\n')

    # credentials = (args[0], args[1])
    # account = Account(credentials)
    #
    # if account.authenticate(scopes=['basic', 'message_all']):
    #   print('Authenticated!')
    # mailbox = account.mailbox()
    # message = account.mailbox()
    # message.subject = ""Project Cost Rate Changes

    # message.body = message_

    # message.to.add(args[4])
    # await message.send()

    await asyncio.sleep(0.01)






