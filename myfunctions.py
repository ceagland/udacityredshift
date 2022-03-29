import configparser
import boto3
import pandas as pd

def prettyRedshiftProps(props):
    """
    Limit redshift properties to most important for easy review and save results in a 'pretty' format
    
    Parameters:
    props (botocore redshift object): With info to connect to redshift cluster to obtain details
    
    Returns:
    pandas DataFrame with redshift cluster details
    """
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

def get_cluster_props(config):
    """
    Save Redshift Cluster Properties for Review
    
    Parameters:
    config (configparser object): The AWS information (region/access key/secret access key)
    necessary to communicate with AWS via boto3.
    
    Returns:
    pandas DataFrame with redshift cluster details
    """
    redshift = boto3.client('redshift',
                            region_name = config.get('AWS','REGION'),
                            aws_access_key_id=config.get('AWS','KEY'),
                            aws_secret_access_key=config.get('AWS','SECRET'))
    try:
        cluster_details_raw = redshift.describe_clusters(ClusterIdentifier=config.get('CLUSTER', 'RS_Identifier'))['Clusters'][0]
        cluster_details_pretty = prettyRedshiftProps(cluster_details_raw)
        return cluster_details_pretty
    except:
        print('Cluster not found')
    
    
    
    
