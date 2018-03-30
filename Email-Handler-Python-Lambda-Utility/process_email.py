import email
import os
import boto3
import time
import urllib
import datetime
import requests
import json
import constants
import datetime
import uuid
import xmltodict
import re
import ast

local_folder = "/tmp/"



def get_file_size(file_name):
    return os.stat(file_name).st_size


def get_email_body(_email):
    body = ""
    if _email.is_multipart():
        for part in _email.walk():
            ctype = part.get_content_type()
            cdispo = str(part.get('Content-Disposition'))

            # skip any text/plain (txt) attachments
            if ctype == 'text/plain' and 'attachment' not in cdispo:
                body = part.get_payload(decode=True)  # decode
                break
    # not multipart - i.e. plain text, no attachments, keeping fingers crossed
    else:
        body = _email.get_payload(decode=True)
    return body


def check_user_type(email):
    _data = dict()
    _response = dict()
    _data["user_email"] = email
    post_data = requests.post(constants.URL_CHECK_USER_TYPE, data=json.dumps(_data))
    if post_data.status_code == 200:
        _response = json.loads(post_data.text)
    return _response


def handle_user_bill():
    output_file = '/tmp/attachment'
    email_array = []
    msg = email.message_from_file(open('/tmp/input_url'))
    email_data = dict()
    _from = msg['from'].replace(" ", "").split("<")[1].replace(">", "")
    email_data['email_id'] = _from
    email_data['to'] = msg['to']
    email_data['title'] = msg['subject']
    email_data['bill_date'] = datetime.datetime.now().strftime("%Y-%m-%d")
    email_data["auth_token"] = ""
    email_data["description"] = get_email_body(msg)
    email_data["merchant_id"] = ""
    email_data["bill_amount"] = "0"
    email_data["user_type"] = "user"
    attachment = msg.get_payload()
    files = []

    for index in range(len(attachment)):
        if index == 0:
            continue
        else:
            uid = str(uuid.uuid4())
            bill_data = dict()
            bfContentRepositoryDict = dict()
            try:
                original_file_name = attachment[index].get_filename()
                attachment_type = original_file_name.split(".")[-1]
            except Exception as e:
                attachment_type = 'x'
            file_name = "email-uploaded-bills/" + uid + "_" + str(int(time.time())) + "_" + original_file_name

            output_file += original_file_name
            open(output_file, 'wb').write(attachment[index].get_payload(decode=True))
            file_size = get_file_size(output_file)
            upload_data_over_s3(output_file, file_name)
            bill_data["file_name"] = original_file_name
            bill_data["unique_name"] = uid + "_" + str(int(time.time())) + "_" + original_file_name
            bill_data["file_extension"] = attachment_type
            bill_data["mime_type"] = attachment[index].get_content_type()
            bill_data["file_path"] = constants.UPLOAD_BUCKET + "/" + file_name
            bill_data["upload_status"] = "y"
            bill_data["size"] = file_size
            bfContentRepositoryDict["BfContentRepository"] = bill_data
            files.append(bfContentRepositoryDict)
    email_data["files"] = files
    #print email_datas
    cnt = 3
    for index in range(cnt):
        print email_data
        post_data = requests.post(constants.SEND_ATTECHMENT_URL, data=json.dumps(email_data))
        if post_data.status_code == 200:
            print "GOT SUCCESS CODE IS " + str(post_data.status_code)
            print post_data.text
            break
        else:
            print "GOT ERROR CODE IS " + str(post_data.status_code)
            print post_data.text
            time.sleep(10)
        cnt -= 1
    if cnt == 0:
        with open("/tmp/errorFile", "w+") as error_file:
            json.dump(email_array, error_file)
        error_file_name = "api/errors/" + str(uuid.uuid4()) + "_" + str(int(time.time()))
        upload_data_over_s3("/tmp/errorFile", error_file_name)


def handle_merchant_bill(user_id):
    output_file = '/tmp/attachment'
    email_array = []
    msg = email.message_from_file(open('/tmp/input_url'))
    email_data = dict()
    _from = msg['from'].replace(" ", "").split("<")[1].replace(">", "")
    email_data['email_id'] = ""
    email_data['to'] = msg['to']
    email_data['title'] = msg['subject']
    email_data["auth_token"] = ""
    email_data["merchant_id"] = str(user_id)
    email_data["user_type"] = "merchant"
    attachment = msg.get_payload()
    files = []
    file_name = ""
    file_size = ""
    pdf_file_name = ""
    uid = str(uuid.uuid4())
    if len(attachment) == 0:
        exit(0)

    for index in range(len(attachment)):
        if index == 0:
            continue
        else:
            try:
                original_file_name = attachment[index].get_filename()
                attachment_type = original_file_name.split(".")[-1]
            except Exception as e:
                attachment_type = 'x'
            if attachment_type == "pdf":
                file_name = "email-uploaded-bills/" + uid + "_" + str(int(time.time())) + "_" + original_file_name
                output_file += original_file_name
                open(output_file, 'wb').write(attachment[index].get_payload(decode=True))
                upload_data_over_s3(output_file, file_name)
                upload_data_over_s3(output_file, "busy-merchant-files/pdf-files/" + user_id + "/" + uid + "_" + original_file_name,
                                    constants.TALLY_BUCKET)
                file_size = get_file_size(output_file)
                pdf_file_name = original_file_name

    for index in range(len(attachment)):
        if index == 0:
            continue
        else:
            try:
                original_file_name = attachment[index].get_filename()
                attachment_type = original_file_name.split(".")[-1]
            except Exception as e:
                attachment_type = 'x'
            # if attachment_type == "pdf":
            #     file_name = "email-uploaded-bills/" + uid + "_" + str(int(time.time())) + "_" + original_file_name
            #     output_file += original_file_name
            #     open(output_file, 'wb').write(attachment[index].get_payload(decode=True))
            #     upload_data_over_s3(output_file, file_name)
            #     file_size = get_file_size(output_file)
            #     pdf_file_name = original_file_name
            if attachment_type == "xml":
                ########have to upload xml file over s3######
                output_file += original_file_name
                open(output_file, 'wb').write(attachment[index].get_payload(decode=True))
                upload_data_over_s3(output_file,
                                    "busy-merchant-files/xml-files/" + user_id + "/" + uid + "_" + original_file_name,
                                    constants.TALLY_BUCKET)
                with open(output_file) as _file:
                    try:
                        data = _file.read()
                        data = data.decode('utf-8', 'ignore').encode("utf-8")
                        data = xmltodict.parse(data)
                        json.dumps(data)
                        print json.dumps(data)

                        try:
                            user_phone = data['Sale']['BillingDetails']['MobileNo']
                            user_phone = getPhoneNumber(user_phone)
                        except:
                            try:
                                user_phone = data['Sale']['BillingDetails']['Address1']
                                user_phone = getPhoneNumber(user_phone)
                            except:
                                user_phone = "9999999999"
                        if user_phone is None:
                            try:
                                user_phone = data['Sale']['BillingDetails']['Address1']
                                user_phone = getPhoneNumber(user_phone)
                                if user_phone is None:
                                    user_phone = "999999999"
                            except:
                                user_phone = "9999999999"
                        try:
                            bill_amount = int(ast.literal_eval(data['Sale']['AccEntries']
                                                               ['AccDetail'][0]['AmtMainCur'])) * -1
                        except:
                            bill_amount = 0
                        description = ""
                        try:
                            items = data['Sale']['ItemEntries']['ItemDetail']
                            if isinstance(items, dict):
                                description += items["ItemName"]
                                try:

                                    item_serial_no = items['ItemSerialNoEntries']['ItemSerialNoDetail']
                                    serial_no = ""
                                    if isinstance(item_serial_no, dict):
                                        serial_no = str(item_serial_no['SerialNo']).rstrip()
                                    else:
                                        for serial in item_serial_no:
                                            serial_no += str(serial.get("SerialNo")).rstrip() + ", "
                                    description += ": " + serial_no + "\n\n"

                                except Exception as e:
                                    print e.message
                                    description += "\n"

                            else:
                                for item in items:
                                    description += item["ItemName"]
                                    try:
                                        item_serial_no = item['ItemSerialNoEntries']['ItemSerialNoDetail']
                                        serial_no = ""
                                        if isinstance(item_serial_no, dict):
                                            serial_no = str(item_serial_no['SerialNo']).rstrip()
                                        else:
                                            for serial in item_serial_no:
                                                serial_no += str(serial.get("SerialNo")).rstrip() + ", "
                                        description += ": " + serial_no + "\n\n"
                                    except Exception as e:
                                        print e.message
                                        description += "\n"
                        except:
                            pass
                        try:
                            bill_date = str(data['Sale']['Date'])
                        except:
                            bill_date = datetime.datetime.now().strftime("%Y-%m-%d")
                        try:
                            datetime_object = datetime.datetime.strptime(bill_date, "%d-%m-%Y")
                            if datetime_object.date() == datetime.datetime.today().date():
                                bill_date = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
                            else:
                                bill_date = datetime_object.strftime('%Y-%m-%d') + " 23:59:59"
                        except Exception as e:
                            print e.message
                            bill_date = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
                        bill_files = []
                        bf_content_repository = dict()
                        bf_content_repository["file_name"] = str(pdf_file_name)
                        bf_content_repository["unique_name"] = uid + "_" + \
                                                               str(int(time.time())) + "_" + \
                                                               pdf_file_name
                        bf_content_repository["file_extension"] = "pdf"
                        bf_content_repository["mime_type"] = "application/pdf"
                        bf_content_repository["file_path"] = constants.UPLOAD_BUCKET + "/" + str(file_name)
                        bf_content_repository["upload_status"] = "y"
                        bf_content_repository["size"] = file_size
                        file_data = dict()
                        file_data["BfContentRepository"] = bf_content_repository
                        bill_files.append(file_data)

                        email_data["description"] = str(description)
                        email_data["user_phone"] = str(user_phone)
                        email_data["bill_amount"] = str(bill_amount)
                        email_data["date_added"] = str(bill_date)
                        email_data["files"] = bill_files
                    except Exception as e:
                        print e.message
                        email_data["files"] = bill_files
    cnt = 3
    for index in range(cnt):
        print email_data
        post_data = requests.post(constants.SEND_ATTECHMENT_URL, data=json.dumps(email_data))
        if post_data.status_code == 200:
            print "SUCCESS"
            print post_data.text
            break
        else:
            print "got error:::" + str(post_data.status_code)
            print post_data.text
            time.sleep(10)
        cnt -= 1

    if cnt == 0:
        with open("/tmp/errorFile", "w+") as error_file:
            json.dump(email_data, error_file)
        error_file_name = "api/errors/" + str(uuid.uuid4()) + "_" + str(int(time.time()))
        upload_data_over_s3("/tmp/errorFile", error_file_name)


def getPhoneNumber(str_to_match):
    regex = re.compile(r'^(\s*(((\+91|0|91)?\s*-?\s*[6789](\d{9}(\s|-|$)|(\d{4}(\s*-?\s*)\d{5}(\s|-|$))|(\d{2}\s*-?\s*(\d{7}(\s|-|$)|(\d{3}\s*-?\s*\d{4}(\s|-|$))))))|(91\d{8}($|\s|-))))')
    # print(str_to_match)
    # print(type(str_to_match))
    phones = str_to_match.split(",")
    # print(phones)
    user_phone = []
    for phone in phones:
        # print(phone)
        for i in range(0,len(phone)):
            if(phone[i] == '+' or (phone[i]>='0' and phone[i]<='9')):
                # print("Phone = "+phone)
                phone = phone[i:len(phone)]
                # print("New Phone = "+phone)
                break
        m=regex.match(phone)
        if m is not None:
            user_phone = m.group()
            user_phone = user_phone.replace('+91','')
            user_phone = user_phone.replace(' ','')
            user_phone = user_phone.replace('-','')
            if user_phone[0] == '0':
                user_phone = user_phone[1:]
            if len(user_phone)==12 and user_phone[0] == '9' and user_phone[1] == '1':
                user_phone = user_phone[2:]
            print("User Phone = "+user_phone)
            return user_phone
    return None

def process_collected_email_data():
    msg = email.message_from_file(open('/tmp/input_url'))
    email_data = dict()
    _from = msg['from'].replace(" ", "").split("<")[1].replace(">", "")
    email_data['email_id'] = _from
    email_sender_type = check_user_type(_from)
    error = str(email_sender_type.get("error"))
    user_type = str(email_sender_type.get("user_type"))
    user_id = str(email_sender_type.get("user_id"))
    if error == "False" and user_type == "merchant":
        # call function that will take care of bills
        handle_merchant_bill(user_id)
    elif error == "False" and user_type == "user":
        # call function that will take care of bills
        handle_user_bill()
    else:
        handle_user_bill()
    return _from


def delete_file(file_name):
    try:
        client = boto3.client('s3')
        client.delete_object(Bucket=constants.DOWNLOAD_BUCKET, Key=file_name)
    except Exception as e:
        print e.message


def lambda_handler(event, context):
    start_time = time.time()
    try:
        os.remove("/tmp/input_url")
    except Exception as e:
        pass
    try:
        os.remove("/tmp/attachment")
    except Exception as e:
        pass

    print "got event " + str(event)

    

    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key']).decode('utf8')
    input_url = local_folder + "input_url"
    s3.Bucket(constants.DOWNLOAD_BUCKET).download_file(key, input_url)
    print "file downloaded sucessfully"
    _from = process_collected_email_data()
    upload_data_over_s3(input_url, "user-emails/" + _from + "/" + key.split("/")[-1], constants.TALLY_BUCKET)
    delete_file(key)
    print("--- %s total execution time ---" % (time.time() - start_time))


if __name__ == '__main__':
    lambda_handler(constants.event, "")
