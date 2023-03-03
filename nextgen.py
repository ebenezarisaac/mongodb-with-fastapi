import os
import time
from datetime import datetime
from pprint import pprint
from typing import List, Optional
from xmlrpc.client import DateTime

import motor.motor_asyncio
import pytz
import uvicorn
from bson import ObjectId
from dateutil import parser
from fastapi import Body, FastAPI, HTTPException, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, EmailStr, Field
from pymongo import MongoClient
from aggregation_queries import *

utc=pytz.UTC


mongo_client = MongoClient('mongodb+srv://devopsninjamongo:xuaMXdtzKi00QrR0@cluster0.amj8hrc.mongodb.net/test')

app = FastAPI()

db = mongo_client['nextgendb']
deployments_collection = db.deployments

class Deployment(BaseModel):
    name: str
    dev_deployment_count: int
    int_deployment_count: int
    prod_deployment_count: int
    date_captured_on: datetime

class PyObjectId(ObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not ObjectId.is_valid(v):
            raise ValueError("Invalid objectid")
        return ObjectId(v)

    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema.update(type="string")

class DeploymentStat(BaseModel):
    # id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")
    _id: str
    total_dev_deployment_count: int
    total_int_deployment_count: int
    total_prod_deployment_count: int

@app.get(
    "/deployments", response_description="List all deployments", response_model=List[Deployment]
)
async def list_deployments():
    start_time = time.time()
    deployments = deployments_collection.find()
    filtered_deployments = []
    for deployment in deployments:
        if utc.localize(deployment['date_captured_on']) >= parser.parse("2022-09-01T00:03:46Z"):
            filtered_deployments.append(deployment)
    print(f'Count: {len(filtered_deployments)}')
    print(f"Time took: {round((time.time() - start_time), 2)}secs")
    return filtered_deployments

pipeline = [
   stage_match_last_thirtydays, 
   stage_group_and_calculate_deploymentcounts,
   stage_limit_5
]

@app.get(
    "/deploymentsByAggregation", response_description="List all deployments"
)
async def list_deployments_byaggregation():
    start_time = time.time()
    deployments = [{**x, "name": x['_id']} for x in list(deployments_collection.aggregate(pipeline))]
    print(f'Count: {len(deployments)}')
    # for deployment in deployments:
    #     pprint(deployment)
    print(f"Time took: {round((time.time() - start_time), 2)}secs")
    return deployments

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)




