"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)

LINES = ["red", "blue", "green"]
# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# Define a Faust Stream that ingests data from the Kafka Connect stations topic and
# place it into a new topic with only the necessary information.

def transform_station(station: Station):
    """ A helper function that transforms input `Station` records into `TransformedStation` records.
        Note "line" is the color of the station:
        So if the `Station` record has the field `red` set to true, then the `line` of
        the `TransformedStation` will be set to string `"red"`.
    """
    red_blue_green_signature = [station.red, station.blue, station.green]
    true_index = [line[0] for line in enumerate([station.red, station.blue, station.green]) if line[1] == True ]
    if len(true_index) == 0:
        logger.warning(f"no True values in {red_blue_green_signature}")
        return None
    if len(true_index) > 1:
        logger.warning(f"multiple true values in {red_blue_green_signature} at {true_index}")

    line = LINES[true_index[0]]
    return TransformedStation(station_id=station.station_id, station_name=station.station_name, order=station.order, line=line)


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")

# Define the input Kafka Topic
topic = app.topic("pg_stations", value_type=Station)

# Define the output Kafka Topic
out_topic = app.topic("transformed_stations", partitions=1, key_type=int, value_type=TransformedStation)


# Define a Faust Table
table = app.Table(
   "transformed_stations_table",
   default=TransformedStation,
   partitions=1,
   changelog_topic=out_topic,
)

@app.agent(topic)
async def transformed_station_stream(stations):
    async for station in stations:
        t_station = transform_station(station) # transform station
        if t_station is not None:
            # await out_topic.send(key=t_station.station_id, value=t_station)
            table[t_station.station_name] = t_station


if __name__ == "__main__":
    app.main()
