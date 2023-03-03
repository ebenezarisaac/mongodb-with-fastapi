from datetime import datetime
from datetime import timezone


stage_match_last_thirtydays = {
    '$match': {
        'date_captured_on': {
            '$gte': datetime(2022, 9, 1, 0, 0, 0, tzinfo=timezone.utc)
        }
    }
}

stage_group_and_calculate_deploymentcounts = {
    '$group': {
        '_id': '$name', 
        'total_dev_deployment_count': {
            '$sum': '$dev_deployment_count'
        }, 
        'total_int_deployment_count': {
            '$sum': '$int_deployment_count'
        }, 
        'total_prod_deployment_count': {
            '$sum': '$prod_deployment_count'
        }
    }
}
# Limit to 5 document:
stage_limit_5 = { "$limit": 5 }