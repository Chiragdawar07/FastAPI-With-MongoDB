#importing all required modules
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pymongo import MongoClient
from typing import List
import json
from fastapi.responses import JSONResponse


#MongoDB setup
MONGO_URI = "mongodb://localhost:27017"
DATABASE_NAME = "mydatabase"
COLLECTION_NAME = "mycollection"

#MongoDB connection
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

#Using Pydantic model
class Item(BaseModel):
    id: int
    name: str
    description: str

#Creating instance of FastAPI
app = FastAPI()

#Performing CRUD Operations

#Create operation (POST)
@app.post("/items/", response_model=Item)
def create_item(item: Item):
    try:
        # Insert the item into MongoDB
        result = collection.insert_one(item.model_dump(exclude_unset=True))
        # Fetch the inserted item from the database
        inserted_item =  collection.find_one({"_id": result.inserted_id})
        if inserted_item:
            return inserted_item
        else:
            raise HTTPException(status_code=404, detail="Item not found after insertion")
    except Exception as e:
        print(f"Error creating item: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

#Read operation (GET)
@app.get("/items/{id}")
def read_item(id: int):
    try:
        item = collection.find_one({"id": id})
        if item:
            # Convert ObjectId to string
            item["_id"] = str(item["_id"])
            # Serialize item to JSON format with proper formatting
            json_item = json.dumps(item, indent=4)
            return json_item
        else:
            raise HTTPException(status_code=404, detail="Item not found")
    except Exception as e:
        print(f"Error reading item: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/items/", response_model=List[Item])
def read_items():
    try:
        # Retrieve all items from the database
        items = list(collection.find())
        return items
    except Exception as e:
        print(f"Error reading items: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

#Update operation (PUT)
@app.put("/items/{id}", response_model=Item)
def update_item(id: int, item: Item):
    try:
        # Check if item with given id exists
        existing_item = collection.find_one({"id": id})
        if existing_item:
            # Update the item in MongoDB
            collection.update_one({"id": id}, {"$set": item.model_dump()})
            # Fetch the updated item from the database
            updated_item = collection.find_one({"id": id})

            # Convert ObjectId to string
            updated_item["_id"] = str(updated_item["_id"])
            # Return the updated item
            return updated_item

        else:
            raise HTTPException(status_code=404, detail="Item not found")
    except Exception as e:
        print(f"Error updating item: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

#Delete operation (DELETE)
@app.delete("/items/{id}", response_model=Item)
def delete_item(id: int):
    try:
        # Check if item with given id exists
        existing_item = collection.find_one({"id": id})
        if existing_item:
            # Delete the item from MongoDB
            deleted_item = collection.find_one_and_delete({"id": id})
            return deleted_item
        else:
            raise HTTPException(status_code=404, detail="Item not found")
    except Exception as e:
        print(f"Error deleting item: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

#Aggregate operation (AGGREGATE)
@app.get("/aggregate/")
def aggregate_items():
    try:
        # Perform aggregation operation
        pipeline = [
            # Group documents by the id field and count the number of documents in each group
            {"$group": {"_id": "$id", "total_count": {"$sum": 1}}},
            # Sort the result by total_count in descending order
            {"$sort": {"total_count": -1}}
        ]
        result = collection.aggregate(pipeline)

        # Convert aggregation result to a list of dictionaries
        aggregation_result = list(result)

        return JSONResponse(content=aggregation_result)
    except Exception as e:
        print(f"Error performing aggregation: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
