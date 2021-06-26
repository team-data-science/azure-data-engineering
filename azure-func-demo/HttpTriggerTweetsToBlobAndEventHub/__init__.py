import logging
import json
import jsonschema
from jsonschema import validate

import azure.functions as func


# Return value: Set the name property in function.json to $return.
# With this configuration, the function's return value is persisted as an Event Hub message.

# Imperative: Pass a value to the set method of the parameter declared as an Out type.
# The value passed to set is persisted as an Event Hub message.


tweet_schema = {
    "type": "object",
    "properties": {
        "tweet_id": {
            "type": "integer"
        },
        "account_id": {
            "type": "integer"
        },
        "likes": {
            "type": "integer"
        },
        "replies": {
            "type": "integer"
        },
        "retweets": {
            "type": "integer"
        },
        "tweet": {
            "type": "string"
        },
        "time": {
            "type": "integer"
        },
        "year_month_date": {
            "type": "string"
        },
        # "damage_flag": {
        #     "type": "string"
        # },
        # "image_base64": {
        #     "type": "string"
        # },
        # "latitude": {
        #     "type": "number"
        # },
        # "longitude": {
        #     "type": "number"
        # }
    },
    "required": [
        "tweet_id",
        "account_id",
        "likes",
        "replies",
        "retweets",
        "tweet",
        "time",
        "damage_flag",
        # "image_base64",
        "latitude",
        "longitude"
    ]
}


def main(req: func.HttpRequest,
         outputBlob: func.Out[bytes],
         outputEventHubMessage: func.Out[bytes]
         ) -> func.HttpResponse:

    logging.info('Python HTTP trigger function processed a request.')

    try:
        json_data = req.get_body()
        json_loads_py = json.loads(json_data)
        logging.info("JSON sent is valid.")

        validate(instance=json_loads_py, schema=tweet_schema)
        logging.info("JSON schema is valid!")

        outputBlob.set(json_data)
        logging.info('Tweet is written to blob storage.')

        outputEventHubMessage.set(json_data)
        logging.info('Tweet is forwarded to Event Hub.')

        # logging.info('This HTTP triggered function executed successfully.')

        return func.HttpResponse(
            "This HTTP triggered function executed successfully.",
            status_code=200
        )

    except json.JSONDecodeError as err:

        logging.error(f"Invalid JSON passed! Error: {err}")

        return func.HttpResponse(
            f"Invalid json passed! Error: {err}",
            status_code=400
        )

    except jsonschema.exceptions.ValidationError as err:

        logging.error(f"JSON Schema is not valid! Error: {err}")

        return func.HttpResponse(
            f"JSON Schema is not valid! Error: {err}",
            status_code=400
        )

    except ValueError as err:
        logging.error(err)

        return func.HttpResponse(
            f"Error: {err}",
            status_code=400
        )
