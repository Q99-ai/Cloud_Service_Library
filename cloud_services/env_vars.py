import os

#AWS
AWS_KEY = os.environ.get("AWS_KEY")
AWS_SECRET = os.environ.get("AWS_SECRET")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

#testing
AWS_URL = os.environ.get("AWS_URL", None)