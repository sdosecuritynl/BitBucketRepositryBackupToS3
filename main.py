#Python 3.10.6
#Default Encryption: Server-side encryption with Amazon S3 managed keys (SSE-S3)

import os
import glob
import time
import boto3
import requests
import calendar
from datetime import date
from requests.auth import HTTPBasicAuth

class Bitbucket:
    username = ""
    appPassword = ""
    workspace = ""
    url = ""
    page = 1

    def __init__(self, username, appPassword, workspace, url):
        self.username = username
        self.appPassword = appPassword
        self.workspace = workspace
        self.url = url % (workspace, self.page)
        #print(self.url)

    def generateBundleFiles(self):
        ''' Fetching list of all the repositories under <Add Here> workspace '''
        response = requests.get(self.url, auth=HTTPBasicAuth(self.username, self.appPassword))
        page_json = response.json()

        os.system("rm -rf bundles")
        os.mkdir('bundles')
        os.mkdir('clones')
        os.chdir('clones')

        for repo in page_json['values']:
            repositoryName = repo['slug']
            splitRepositoryLink = repo['links']['clone'][0]['href'].split('@')
            splitRepositoryLink = splitRepositoryLink[0] + ':' + self.appPassword + '@' + splitRepositoryLink[1]

            os.system("git clone " + splitRepositoryLink)
            os.chdir(repositoryName)
            os.system("git bundle create " + repositoryName + '-' + str(date.today()) + ".bundle --all")
            os.system("mv " + repositoryName + '-' + str(date.today()) + ".bundle ../../bundles")
            os.chdir("../")
            os.system("rm -rf " + repositoryName)

        if 'next' in page_json:
            self.url = page_json['next']
            self.generateBundleFiles()

        os.chdir("../")
        os.system("rm -rf clones")

class AWS:
    client = ""
    session = ""
    resource = ""
    ACCESS_KEY_ID = ""
    SECRET_ACCESS_KEY = ""
    REGION = ""

    def __init__(self, ACCESS_KEY_ID, SECRET_ACCESS_KEY, REGION, resource):
        self.ACCESS_KEY_ID = ACCESS_KEY_ID
        self.SECRET_ACCESS_KEY = SECRET_ACCESS_KEY
        self.REGION = REGION

        self.createSession()
        self.createResource(resource)
        self.createClient(resource)

    def createSession(self):
        self.session = boto3.Session(
            aws_access_key_id = self.ACCESS_KEY_ID,
            aws_secret_access_key = self.SECRET_ACCESS_KEY
        )

    def createClient(self, resource):
        self.client = self.session.client(resource)

    def createResource(self, resource):
        self.resource = self.session.resource(
                resource,
                aws_access_key_id = self.ACCESS_KEY_ID,
                aws_secret_access_key= self.SECRET_ACCESS_KEY)

    def listBuckets(self):
        for bucket in self.resource.buckets.all():
            print(bucket.name)

    def listObjects(self, bucket):
        response = self.resource.Bucket(bucket).objects.all()
        #for object in response:
        #    print(object.key)

        return response

    def uploadFiles(self, file_path, bucket_name, object_name):
        try:
            self.client.upload_file(file_path, bucket_name, object_name)
            #print(f"File '{file_path}' uploaded successfully to '{bucket_name}/{object_name}'")
            return True
        except Exception as e:
            print(f"Error uploading file '{file_path}' to '{bucket_name}/{object_name}': {str(e)}")
            return False

    def deleteFiles(self, bucket):
        fileName = ""
        #print("Removing backup from: " + calendar.month_name[int(time.strftime("%m")) - 1])
        try:
            for obj in self.listObjects(bucket):
                # Remove previous month backup only if the day of the current month is bigger than 15.
                if ((obj.key).startswith(calendar.month_name[int(time.strftime("%m")) - 1] + '/') and ((int(time.strftime("%d")) > 15)):
                    self.client.delete_object(Bucket = bucket, Key = obj.key)
                    #print(f"File '{obj.key}' deleted successfully from '{bucket}/{obj.key}'")
                    fileName = obj.key
            return True
        except Exception as e:
            print(f"Error deleting file '{fileName}' from '{bucket}/{fileName}': {str(e)}")
            return False

if __name__ == "__main__":
    bitbucketClient = Bitbucket(
        username = '<Add Here>',
        appPassword = '<Add Here>',
        workspace = '<Add Here>',
        url = 'https://api.bitbucket.org/2.0/repositories/%s?pagelen=100&fields=next,values.links.clone.href,values.slug&page=%d')

    awsClient = AWS(
        ACCESS_KEY_ID = '<Add Here>',
        SECRET_ACCESS_KEY = '<Add Here>',
        REGION = '<Add Here>',
        resource = 's3')

    bitbucketClient.generateBundleFiles()

    for file in glob.glob('bundles/*.bundle'):
        awsClient.uploadFiles(file, '<Add Here>', calendar.month_name[int(time.strftime("%m"))] + '/' + file.replace('bundles\\',''))

    awsClient.deleteFiles('<Add Here>')
