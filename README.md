This is the lambda function that invoke when lambda triggers.
It takes the email file coming to S3 bucket and process it through lambda function.
After processing it extract all the useful information from email and hit an api to reply to save the bill information of that particular sender.