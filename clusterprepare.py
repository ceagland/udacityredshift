import configparser
import boto3
import time
import json

from myfunctions import prettyRedshiftProps, get_cluster_props

def create_role(config):   
    """
    Create IAM role with S3 Read Access for AWS.
  
    Create an IAM role, which is necessary for using/interacting with other AWS services.
    Add AmazonS3ReadOnly Policy for reading from S3 bucket.    
  
    Parameters:
    config (configparser object): The AWS information (region/access key/secret access key)
    necessary to communicate with AWS via boto3.
  
    Returns:
    IAM roleARN (Amazon Resource Name for identification)
  
    """
        
    iam = boto3.client('iam',
                       region_name = config.get('AWS', 'REGION'),
                       aws_access_key_id = config.get('AWS','KEY'),
                       aws_secret_access_key = config.get('AWS', 'SECRET'))
    
    # With S3ReadOnly
    try:
        dwhRole = iam.create_role(
            Path='/',
            RoleName=config.get('IAM', 'ROLENAME'),
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )
        
        iam.attach_role_policy(RoleName=config.get('IAM', 'ROLENAME'),
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']

        roleArn = iam.get_role(RoleName=config.get('IAM', 'ROLENAME'))['Role']['Arn']
        print('Role ' + config.get('IAM', 'ROLENAME') + ' created successfully')
    
    except iam.exceptions.EntityAlreadyExistsException as e:
        print('Warning: Role already exists - returned roleArn may be invalid')
        roleArn = iam.get_role(RoleName=config.get('IAM', 'ROLENAME'))['Role']['Arn']
    
    except Exception as e:
        raise e
    
    return(roleArn)

def create_cluster(config, iam_role):
    """
    Create Redshift Cluster
  
    Creates an AWS Redshift cluster based on configuration input.
  
    Parameters:
    config (configparser object): The AWS information (region/access key/secret access key)
    necessary to communicate with AWS via boto3, as well as cluster type/size, database and
    user information necessary for setup.
  
    Returns:
    None 
    """
    redshift = boto3.client('redshift',
                            region_name = config.get('AWS','REGION'),
                            aws_access_key_id=config.get('AWS','KEY'),
                            aws_secret_access_key=config.get('AWS','SECRET'))
    
    try:
        print('Creating Redshift Cluster')
        if config.get('CLUSTER', 'RS_CLUSTER_TYPE') != 'single-node':  
            response = redshift.create_cluster(        
                ClusterType=config.get('CLUSTER', 'RS_CLUSTER_TYPE'),
                NodeType=config.get('CLUSTER', 'RS_NODE_TYPE'),
                NumberOfNodes=int(config.get('CLUSTER', 'RS_NUM_NODES')),
                VpcSecurityGroupIds=[config.get('CLUSTER', 'RS_SECURITYGRP')],
                
                #Identifiers & Credentials
                DBName=config.get('CLUSTER', 'DB_NAME'),
                ClusterIdentifier=config.get('CLUSTER', 'RS_IDENTIFIER'),
                MasterUsername=config.get('CLUSTER', 'RS_USER'),
                MasterUserPassword=config.get('CLUSTER', 'RS_PASSWORD'),

                #Roles (for s3 access)
                IamRoles=[iam_role]
                )
        else:
            response = redshift.create_cluster(        
                ClusterType=config.get('CLUSTER', 'RS_CLUSTER_TYPE'),
                NodeType=config.get('CLUSTER', 'RS_NODE_TYPE'),
                VpcSecurityGroupIds=[config.get('CLUSTER', 'RS_SECURITYGRP')],

                #Identifiers & Credentials
                DBName=config.get('CLUSTER', 'DB_NAME'),
                ClusterIdentifier=config.get('CLUSTER', 'RS_IDENTIFIER'),
                MasterUsername=config.get('CLUSTER', 'RS_USER'),
                MasterUserPassword=config.get('CLUSTER', 'RS_PASSWORD'),

                #Roles (for s3 access)
                IamRoles=[iam_role]
                )            
                
    except Exception as e:
        print(e)
        
    isavail = 0
    while isavail == 0:
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=config.get('CLUSTER', 'RS_Identifier'))['Clusters'][0]
        redshiftprops = prettyRedshiftProps(myClusterProps)
        cluster_status = redshiftprops[redshiftprops['Key'] == 'ClusterStatus']['Value'].iloc[0]
        
        if cluster_status == 'available':
            print('Redshift Cluster Created and Available')
            isavail = 1
        else:
            print('...')
            time.sleep(20)


def main():
    """
    Creates Redshift Cluster, including creation of IAM role with S3 read access
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    iam_role_arn = create_role(config)
    cluster_detail = create_cluster(config, iam_role_arn)


if __name__ == "__main__":
    main()