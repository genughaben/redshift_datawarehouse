import pandas as pd
import boto3
import time
import json
from config import config


class RedshiftCluster:
    '''
    Contains functionality to setup and shutdown a fully functionally Redshift cluster using paramters
    configured in dwh.cfg loaded via config.
    '''


    def __init__(self):
        '''
        Initializes instances of the RedshiftCluster class.
        '''
        self.KEY                    = config.get('AWS','KEY')
        self.SECRET                 = config.get('AWS','SECRET')

        self.DWH_DB                 = config.get("DWH", "DB_NAME")
        self.DWH_DB_USER            = config.get("DWH","DB_USER")
        self.DWH_DB_PASSWORD        = config.get("DWH","DB_PASSWORD")
        self.DWH_PORT               = config.get("DWH","DB_PORT")

        self.DWH_IAM_ROLE_NAME      = config.get("CLUSTER", "DWH_IAM_ROLE_NAME")
        self.DWH_CLUSTER_TYPE       = config.get("CLUSTER", "DWH_CLUSTER_TYPE")
        self.DWH_NUM_NODES          = config.get("CLUSTER", "DWH_NUM_NODES")
        self.DWH_NODE_TYPE          = config.get("CLUSTER", "DWH_NODE_TYPE")
        self.DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER", "DWH_CLUSTER_IDENTIFIER")

        self.BUCKET                 = config.get('S3', 'BUCKET')
        self.LOG_DATA               = config.get('S3', 'LOG_DATA')
        self.LOG_JSONPATH           = config.get('S3', 'LOG_JSONPATH')
        self.SONG_DATA              = config.get('S3', 'SONG_DATA')
                                 
        self.ec2 = boto3.resource(
            'ec2',
            region_name='us-west-2',
            aws_access_key_id=self.KEY,
            aws_secret_access_key=self.SECRET
        )

        self.s3 = boto3.resource(
            's3',
            region_name='us-west-2',
            aws_access_key_id=self.KEY,
            aws_secret_access_key=self.SECRET
        )

        self.iam = boto3.client(
            'iam',
            region_name='us-west-2',
            aws_access_key_id=self.KEY,
            aws_secret_access_key=self.SECRET
        )

        self.redshift = boto3.client(
            'redshift',
            region_name='us-west-2',
            aws_access_key_id=self.KEY,
            aws_secret_access_key=self.SECRET
        )

    def create_iam_role(self):
        '''
        Create the IAM role
        :return: IAM role
        '''
        try:
            print('Creating a new IAM Role')
            path='/'
            role_name=self.DWH_IAM_ROLE_NAME
            description='A role to allow Redshift to access S3'

            trust_policy={
              "Version": "2012-10-17",
              "Statement": [
                {
                  "Action": "sts:AssumeRole",
                  "Effect": "Allow",
                  "Principal": {
                    "Service": "redshift.amazonaws.com"
                  }        # TODO: add parameters for hardware
                }
              ]
            }
            tags=[
                {
                    'Key': 'Environment',
                    'Value': 'Production'
                }
            ]
            dwh_role = self.iam.create_role(
                Path=path,
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description=description,
                MaxSessionDuration=3600,
                Tags=tags
            )
            return dwh_role
        except Exception as e:
            print(e)

    def attach_iam_role_policy(self):
        '''
        Attach Policy policy to IAM role; here: S# read only access.
        :return:
        '''

        print('Attaching Policy')
        self.iam.attach_role_policy(
            PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess',
            RoleName=self.DWH_IAM_ROLE_NAME
        )['ResponseMetadata']['HTTPStatusCode']

    def create_redshift_cluster(self, role_arn):
        '''
        Creates redshift cluster for a given role_arn.
        Automatically reveals ENDPOINT and ARN for dwh.cfg modification in order to make cluster accessable
        for ETL and analytics.
        :param role_arn:
        :return:
        '''
        try:
            response = self.redshift.create_cluster(
                # parameters for hardware
                ClusterType=self.DWH_CLUSTER_TYPE,
                NodeType=self.DWH_NODE_TYPE,
                NumberOfNodes=int(self.DWH_NUM_NODES),

                # add parameters for identifiers & credentials
                ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER,
                DBName=self.DWH_DB,
                MasterUsername=self.DWH_DB_USER,
                MasterUserPassword=self.DWH_DB_PASSWORD,
                Port=int(self.DWH_PORT),

                # add parameter for role (to allow s3 access)
                IamRoles=[role_arn]
            )
        except Exception as e:
            print(e)
        is_available, cluster_props = self.check_cluster_availability()
        while not is_available:
            time.sleep(5)
            is_available, cluster_props = self.check_cluster_availability()
        self.print_cluster_info(cluster_props)
        return cluster_props


    def check_cluster_availability(self):
        '''
        Checks clusters availability and returns props.
        :return:
        '''
        cluster_props = self.redshift.describe_clusters(ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER)["Clusters"][0]
        is_available = cluster_props['ClusterStatus'] == 'available'
        return is_available, cluster_props


    def check_cluster_shutdown(self):
        '''
        Checks clusters availability and returns props.
        :return:
        '''
        cluster_props = self.redshift.describe_clusters(ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER)["Clusters"][0]
        is_deleted = cluster_props['ClusterStatus'] == 'deleted'
        return is_deleted, cluster_props


    def print_cluster_info(self, cluster_props):
        '''
        Displays cluster info from cluster_props.
        :param cluster_props:
        :return:
        '''
        print(f"Cluster {self.DWH_CLUSTER_IDENTIFIER} is online.")
        self.prettify_redshift_props(cluster_props)
        DWH_ENDPOINT = cluster_props['Endpoint']['Address']
        DWH_ROLE_ARN = cluster_props['IamRoles'][0]['IamRoleArn']
        print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
        print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)


    def prettify_redshift_props(self, props):
        '''
        Displays cluster_props.
        :param props:
        :return:
        '''
        pd.set_option('display.max_colwidth', -1)
        keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
        x = [(k, v) for k,v in props.items() if k in keysToShow]
        return pd.DataFrame(data=x, columns=["key", "value"])


    def open_endpoint_to_cluster(self, cluster_props):
        '''
        Opens a connection to a cluster via an tcp endpoint.
        :param cluster_props:
        :return:
        '''
        try:
            vpc = self.ec2.Vpc(id=cluster_props['VpcId'])
            defaultSg = list(vpc.security_groups.all())[0]
            print(defaultSg)

            defaultSg.authorize_ingress(
                GroupName=defaultSg.group_name,
                CidrIp='0.0.0.0/0',
                IpProtocol='TCP',
                FromPort=int(self.DWH_PORT),
                ToPort=int(self.DWH_PORT)
            )
        except Exception as e:
            print(e)


    def delete_redshift_cluster(self):
        '''
        Shuts down and deletes an existing redshift cluster
        :return:
        '''
        self.redshift.delete_cluster(
            ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER,
            SkipFinalClusterSnapshot=True
        )
        is_deleted, cluster_props = self.check_cluster_shutdown()
        while not is_deleted:
            time.sleep(5)
            is_deleted, cluster_props = self.check_cluster_availability()
        print(f"Cluster {self.DWH_CLUSTER_IDENTIFIER} is offline.")


    def delete_role(self, DWH_IAM_ROLE_NAME=None):
        '''
        Deletes IAM role.
        :return:
        '''

        if DWH_IAM_ROLE_NAME is not None:
            self.DWH_IAM_ROLE_NAME = DWH_IAM_ROLE_NAME

        self.iam.detach_role_policy(
            RoleName=self.DWH_IAM_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )
        self.iam.delete_role(
            RoleName=self.DWH_IAM_ROLE_NAME
        )


    def setup(self):
        '''
        Sets up IAM role and redshift cluster as well as creates an endpoint.
        :return:
        '''
        role = self.create_iam_role()
        self.attach_iam_role_policy()
        role_arn = self.iam.get_role(RoleName=self.DWH_IAM_ROLE_NAME)['Role']['Arn']
        cluster_props = self.create_redshift_cluster(role_arn)
        self.open_endpoint_to_cluster(cluster_props)


    def shutdown(self):
        '''
        Shuts down and deletes redshift cluster and IAM role.
        :return:
        '''
        self.delete_redshift_cluster()
        self.delete_role()
