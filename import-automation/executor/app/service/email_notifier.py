from google.appengine.api import mail



class EmailNotifier:
    """Class for sending email notifications about import progress.

    Attributes:
        sender: Sender address as a string.
    """
    def __init__(self, sender: str):
        self.sender = sender

    def send(self, subject: str, body: str, receiver_name: str, receiver_address:):
        mail.send_mail(sender=self.sender,
                       to=f'{receiver_name} <{receiver_address}>',
                       subject=subject,
                       body=body)

