import pymysql
import os
conn = pymysql.connect(host='localhost', user='root', password='1234',
                       db='enron', charset='utf8')
curs = conn.cursor(pymysql.cursors.DictCursor)

def getMessageId(string): #Message-id를 추출하는 함수
    endString="JavaMail.evans@thyme"
    start = string.find("<")
    if(start == -1):
        return False
    start += 1  

    end = string.find(endString)
    if(end == -1):
        return False
    end -= 1
    return string[start:end]

def getDate(string): #날짜 변환함수
    month = {"Jan":1,"Feb":2,"Mar":3,"Apr":4,"May":5,"Jun":6
        ,"Jul":7,"Aug":8,"Sep":9,"Oct":10,"Nov":11,"Dec":12}
    sList =  string.split()
    dateTime = sList[4]+"-"+str(month[sList[3]])+"-"+sList[2]+" "+sList[5]
    return dateTime

def getSender(string): #Sender 추출
    string = string.split()
    if(string[0] != "From:"):
        return False
    return str(isPeople(string[1]))

def isPeople(email): #sender나 receiver에서 추출한 email과 매치되는 사람을 db에서 찾고 없으면 저장
    sql = "select * from people where email=%s"
    curs.execute(sql,(email))
    data = curs.fetchall()
    if(len(data) != 0):
        return data[0]['id']

    insert_people(email)
    curs.execute(sql,(email))
    data = curs.fetchall()
    return data[0]['id']


def getReceiver(string,f): #receiver, direct 추출
    if(string[:4] != "To: "): #형식에 맞지 않으면 False 반환
        return False, False
    string = string[4:]

    while True: #다수 고려
        newLine = f.readline()
        if(newLine.split()[0] == "Subject:"):
            break
        string += newLine
    string = string.replace(" ","")
    emails = string.split(",")
    
    emails = list(map(lambda email: email.strip(), emails))#공백 제거
    data = []
    for email in emails:
        data.append(str(isPeople(email)))
    
    direct = 1
    if(len(data)>1):
        direct = 0
    return " ".join(data), direct

def insert_email(data): #email table에 주어진 값들을 삽입
    sql = "insert into email (message_id, date_time, sender, receiver, direct)  values (%s,%s,%s,%s,%s)"
    curs.execute(sql,data)
    conn.commit()

def insert_people(email): #people table에 주어진 값들을 삽입
    sql = "insert into people (name, email) values (%s, %s)"
    curs.execute(sql,(email[:email.find("@")],email))
    conn.commit()


def executeDb(f): #이메일 파일 하나를 분석하는 함수
    data_email = []
    data_email.append(getMessageId(f.readline()))
    data_email.append(getDate(f.readline()))
    data_email.append(getSender(f.readline()))

    receiver = getReceiver(f.readline(),f)
    data_email.append(receiver[0])
    data_email.append(receiver[1])
    for data in data_email[:-2]:
        if(data == False):
            print(f.name,data_email)
            return False

    insert_email(data_email)


def analyzeFiles(): #분석 main 함수
    for i in range(1, 9):
        dirname = ".\\data\\"+str(i)
        filenames = os.listdir(dirname)
        for filename in filenames:
            if os.path.splitext(filename)[-1] != '.txt':
                continue
            full_filename = os.path.join(dirname,filename)

            f= open(full_filename)
            executeDb(f)
            f.close()

def directCheck(): 
    sql = "select sender,receiver,direct from email"
    curs.execute(sql)
    emails = curs.fetchall()
    print(len(emails))
    for email in emails:
        if(email['direct'] == '1'):
            directUpdate(int(email['receiver']))
        else:
            broadUpdate(int(email['sender']))

def directUpdate(id):
    direct_sql = "update people set direct_count = direct_count + 1 where id=%s"
    curs.execute(direct_sql,(id))
    conn.commit()
    
def broadUpdate(id):
    broad_sql = "update people set broad_count = broad_count + 1 where id=%s"
    curs.execute(broad_sql,(id))
    conn.commit()

def assignment2():
    sql = "select * from people order by direct_count desc"
    curs.execute(sql)
    direct_emails = curs.fetchall()

    sql = "select * from people order by broad_count desc"
    curs.execute(sql)
    broad_emails = curs.fetchall()


    print(broad_emails[0]['name'],"sent",broad_emails[0]['broad_count'],"broadcast mails")
    print(direct_emails[0]['name'],"sent",direct_emails[0]['direct_count'],"direct mails")

def assignment1(date,id):
    count = 0
    sql = "select receiver from email where date_time between '"+date+" 00:00:00'"+" and '"+date+ " 23:59:59'"
    print(sql)
    curs.execute(sql)
    data = curs.fetchall()
    if str(id) in data[0]['receiver'].split():
        count += 1
    print(count)


#analyzeFiles()
#directCheck()
assignment2()
assignment1("2000-09-14",51)