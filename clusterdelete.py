import configparser
import boto3
import time
        
def delete_role(config):
    """
    Deleting IAM roles (by first deleting policies)
  
    To delete an IAM role, the associated policies have to be deleted with it.
    This function removes everything necessary to fully delete the IAM role.
  
    Parameters:
    config (configparser object): The AWS information (region/access key/secret access key)
    necessary to communicate with AWS via boto3.
  
    Returns:
    None  
    """
    try:
        iam = boto3.client('iam',
                            region_name = config.get('AWS','REGION'),
                            aws_access_key_id = config.get('AWS','KEY'),
                            aws_secret_access_key = config.get('AWS', 'SECRET'))

        existing_policies = iam.list_attached_role_policies(RoleName = config.get('IAM', 'ROLENAME'))['AttachedPolicies']

        if len(existing_policies) > 0:
            for policyidx in range(len(existing_policies)):
                iam.detach_role_policy(RoleName=config.get('IAM', 'ROLENAME'), PolicyArn=existing_policies[policyidx]['PolicyArn'])

        iam.delete_role(RoleName=config.get('IAM', 'ROLENAME'))

        print('Role ' + config.get('IAM', 'ROLENAME') + ' successfully deleted')
    except Exception as e:
        raise e
        
def delete_cluster(config):
    """
    Deletes redshift cluster
    
    Parameters:
    config (configparser object): The AWS information (region/access key/secret access key)
    necessary to communicate with AWS via boto3.
    
    Returns:
    None
    """
    redshift = boto3.client('redshift',
                            region_name = config.get('AWS','REGION'),
                            aws_access_key_id=config.get('AWS','KEY'),
                            aws_secret_access_key=config.get('AWS','SECRET'))
    
    try:
        redshift.delete_cluster(ClusterIdentifier=config.get('CLUSTER','RS_IDENTIFIER'),  SkipFinalClusterSnapshot=True)
    except Exception as e:
        raise e
        
    clusterfound = 1
    while (clusterfound == 1):
        try:
            redshift.describe_clusters(ClusterIdentifier=config.get('CLUSTER', 'RS_Identifier'))['Clusters'][0]
        except Exception as e:
            print('Cluster not found', e)
            clusterfound = 0
    
    print('Cluster deleted')    

def main():
    """
    Deletes IAM role and redshift cluster. IAM role mostly deleted to ensure newly-created role in the future
    """
    print("Starting cluster deletion")
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    delete_role(config)
    delete_cluster(config)   
    
    
if __name__ == "__main__":
    main()