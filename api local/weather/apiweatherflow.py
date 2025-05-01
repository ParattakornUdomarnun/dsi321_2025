import os
import time
import datetime
import requests
import pandas as pd
import pyarrow
from prefect import flow, task # Prefect flow and task decorators
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

@task
def fetch_weather_data(lat,lon):
    api_key = "d7d117e39072b242365b3a6acc9b7d1b"  # Replace with your OpenWeatherMap API key
    
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}"
    
    try:
        response = requests.get(url)
        data = response.json()

        dt = data.get("dt")
        # Convert timestamp to UTC
        utc_time = datetime.fromtimestamp(dt, tz=timezone.utc)

        # Convert to Asia/Bangkok timezone
        bangkok_time = utc_time.astimezone(ZoneInfo("Asia/Bangkok"))

        weather_data = {
            "coord.lon": data["coord"].get("lon"),
            "coord.lat": data["coord"].get("lat"),
            "weather.id": data["weather"][0].get("id"),
            "weather.main": data["weather"][0].get("main"),
            "weather.description": data["weather"][0].get("description"),
            "weather.icon": data["weather"][0].get("icon"),
            "base": data.get("base"),
            "main.temp": data["main"].get("temp"),
            "main.feels_like": data["main"].get("feels_like"),
            "main.pressure": data["main"].get("pressure"),
            "main.humidity": data["main"].get("humidity"),
            "main.temp_min": data["main"].get("temp_min"),
            "main.temp_max": data["main"].get("temp_max"),
            "main.sea_level": data["main"].get("sea_level"),
            "main.grnd_level": data["main"].get("grnd_level"),
            "visibility": data.get("visibility"),
            "wind.speed": data["wind"].get("speed"),
            "wind.deg": data["wind"].get("deg"),
            "wind.gust": data["wind"].get("gust"),
            "clouds.all": data["clouds"].get("all"),
            "rain.1h": data.get("rain", {}).get("1h"),
            "snow.1h": data.get("snow", {}).get("1h"),
            "dt": bangkok_time,
            "acq_date" : bangkok_time.date(),
            "acq_year" : bangkok_time.strftime('%Y'),
            "acq_month" :bangkok_time.strftime('%m'),
            "acq_day" : bangkok_time.strftime('%d'),
            "acq_hour" : bangkok_time.strftime('%H'),
            "acq_minute" : bangkok_time.strftime('%M'),
            "acq_time" : bangkok_time.strftime('%H:%M:%S'),
            "sys.type": data["sys"].get("type"),
            "sys.id": data["sys"].get("id"),
            "sys.country": data["sys"].get("country"),
            "sys.sunrise": data["sys"].get("sunrise"),
            "sys.sunset": data["sys"].get("sunset"),
            "timezone": data.get("timezone"),
            "id": data.get("id"),
            "name": data.get("name"),
            "cod": data.get("cod")
        }

        return weather_data
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None
    except KeyError as e:
        print(f"Error processing data: Missing key {e}")
        return None
    

@flow(name="apiweather-flow", log_prints=True)
def apiweather_flow() :
    df = pd.DataFrame()
    provinces = [
    {"province": "กรุงเทพมหานคร", "lat": 13.7278956, "lon": 100.52412349999997},
    {"province": "กระบี่", "lat": 8.0862997, "lon": 98.90628349999997},
    {"province": "กาญจนบุรี", "lat": 14.0227797, "lon": 99.53281149999998},
    {"province": "กาฬสินธุ์", "lat": 16.4314078, "lon": 103.5058755},
    {"province": "กำแพงเพชร", "lat": 16.4827798, "lon": 99.52266179999992},
    {"province": "ขอนแก่น", "lat": 16.4321938, "lon": 102.8236214},
    {"province": "จันทบุรี", "lat": 12.6112485, "lon": 102.1037809},
    {"province": "ฉะเชิงเทรา", "lat": 13.6904194, "lon": 101.0779594},
    {"province": "ชลบุรี", "lat": 13.3611431, "lon": 100.9846717},
    {"province": "ชัยนาท", "lat": 15.185197, "lon": 100.125125},
    {"province": "ชัยภูมิ", "lat": 15.8068171, "lon": 102.0315027},
    {"province": "ชุมพร", "lat": 10.4930496, "lon": 99.18001989999999},
    {"province": "เชียงราย", "lat": 19.9104798, "lon": 99.840576},
    {"province": "เชียงใหม่", "lat": 18.7882778, "lon": 98.9852902},
    {"province": "ตรัง", "lat": 7.5644836, "lon": 99.623833},
    {"province": "ตราด", "lat": 12.2414391, "lon": 102.5099156},
    {"province": "ตาก", "lat": 16.883571, "lon": 99.125849},
    {"province": "นครนายก", "lat": 14.2069469, "lon": 101.2130514},
    {"province": "นครปฐม", "lat": 13.8199204, "lon": 100.0621676},
    {"province": "นครพนม", "lat": 17.3920394, "lon": 104.7695508},
    {"province": "นครราชสีมา", "lat": 14.9798997, "lon": 102.0977693},
    {"province": "นครศรีธรรมราช", "lat": 8.4324834, "lon": 99.9599032},
    {"province": "นครสวรรค์", "lat": 15.7040292, "lon": 100.1371721},
    {"province": "นนทบุรี", "lat": 13.8621125, "lon": 100.5143528},
    {"province": "นราธิวาส", "lat": 6.4254607, "lon": 101.8253141},
    {"province": "น่าน", "lat": 18.7756317, "lon": 100.7730412},
    {"province": "บึงกาฬ", "lat": 18.3609107, "lon": 103.6464463},
    {"province": "บุรีรัมย์", "lat": 14.9930011, "lon": 103.1029191},
    {"province": "ปทุมธานี", "lat": 14.0208391, "lon": 100.5250276},
    {"province": "ประจวบคีรีขันธ์", "lat": 11.8122853, "lon": 99.7972171},
    {"province": "ปราจีนบุรี", "lat": 14.0469228, "lon": 101.3713265},
    {"province": "ปัตตานี", "lat": 6.7618308, "lon": 101.3232549},
    {"province": "พระนครศรีอยุธยา", "lat": 14.3692354, "lon": 100.5876634},
    {"province": "พะเยา", "lat": 19.2148736, "lon": 100.2023694},
    {"province": "พังงา", "lat": 8.4501372, "lon": 98.5255317},
    {"province": "พัทลุง", "lat": 7.6176119, "lon": 100.0779285},
    {"province": "พิจิตร", "lat": 16.4514727, "lon": 100.3462677},
    {"province": "พิษณุโลก", "lat": 16.8211232, "lon": 100.2658516},
    {"province": "เพชรบุรี", "lat": 13.1119631, "lon": 99.944675 },
    {"province": "เพชรบูรณ์", "lat": 16.4198289, "lon": 101.1605632},
    {"province": "แพร่", "lat": 18.1445779, "lon": 100.1402837},
    {"province": "ภูเก็ต", "lat": 7.9519331, "lon": 98.3380884},
    {"province": "มหาสารคาม", "lat": 16.0145455, "lon": 103.1615169},
    {"province": "มุกดาหาร", "lat": 16.5405865, "lon": 104.7103909},
    {"province": "แม่ฮ่องสอน", "lat": 19.3020292, "lon": 97.9654368},
    {"province": "ยโสธร", "lat": 15.792641, "lon": 104.1452823},
    {"province": "ยะลา", "lat": 6.5411478, "lon": 101.2800155},
    {"province": "ร้อยเอ็ด", "lat": 16.0538195, "lon": 103.6520038},
    {"province": "ระนอง", "lat": 9.9528702, "lon": 98.6084641},
    {"province": "ระยอง", "lat": 12.681395, "lon": 101.2816262},
    {"province": "ราชบุรี", "lat": 13.5282891, "lon": 99.8134211},
    {"province": "ลพบุรี", "lat": 14.799508, "lon": 100.6533701},
    {"province": "ลำปาง", "lat": 18.285539, "lon": 99.5127895},
    {"province": "ลำพูน", "lat": 18.5744626, "lon": 99.0087224},
    {"province": "ศรีสะเกษ", "lat": 15.1182044, "lon": 104.3220095},
    {"province": "สกลนคร", "lat": 17.1672851, "lon": 104.1485681},
    {"province": "สงขลา", "lat": 7.1897003, "lon": 100.5953829},
    {"province": "สตูล", "lat": 6.6238158, "lon": 100.0673744},
    {"province": "สมุทรปราการ", "lat": 13.5990961, "lon": 100.5998319},
    {"province": "สมุทรสงคราม", "lat": 13.4098217, "lon": 100.00226450000002},
    {"province": "สมุทรสาคร", "lat": 13.5475216, "lon": 100.27439559999992},
    {"province": "สระแก้ว", "lat": 13.824038, "lon": 102.0645839},
    {"province": "สระบุรี", "lat": 14.5289154, "lon": 100.91014210000004},
    {"province": "สิงห์บุรี", "lat": 14.8936253, "lon": 100.39673140000002},
    {"province": "สุโขทัย", "lat": 17.0055573, "lon": 99.82637120000004},
    {"province": "สุพรรณบุรี", "lat": 14.4744892, "lon": 100.11771279999994},
    {"province": "สุราษฎร์ธานี", "lat": 9.1382389, "lon": 99.32174829999995},
    {"province": "สุรินทร์", "lat": 14.882905, "lon": 103.49371070000008},
    {"province": "หนองคาย", "lat": 17.8782803, "lon": 102.74126380000008},
    {"province": "หนองบัวลำภู", "lat": 17.2218247, "lon": 102.42603680000002},
    {"province": "อ่างทอง", "lat": 14.5896054, "lon": 100.45505200000002},
    {"province": "อำนาจเจริญ", "lat": 15.8656783, "lon": 104.62577740000006},
    {"province": "อุดรธานี", "lat": 17.4138413, "lon": 102.78723250000009},
    {"province": "อุตรดิตถ์", "lat": 17.6200886, "lon": 100.09929420000005},
    {"province": "อุทัยธานี", "lat": 15.3835001, "lon": 100.02455269999996},
    {"province": "อุบลราชธานี", "lat": 15.2286861, "lon": 104.85642170000006},
    {"province": "บึงกาฬ", "lat": 18.3609104, "lon": 103.64644629999998},
    ]

    for i in provinces :
        new_data = fetch_weather_data(i["lat"],i["lon"])
        df = pd.concat([df, pd.DataFrame([new_data])], ignore_index=True)

    # lakeFS credentials from your docker-compose.yml
    ACCESS_KEY = "access_key"
    SECRET_KEY = "secret_key"

    # lakeFS endpoint (running locally)
    lakefs_endpoint = "http://lakefs-dev:8000/"

    # lakeFS repository, branch, and file path
    repo = "weather"
    branch = "main"
    path = "weather.parquet"

    # Construct the full lakeFS S3-compatible path
    lakefs_s3_path = f"s3a://{repo}/{branch}/{path}"

    # Configure storage_options for lakeFS (S3-compatible)
    storage_options = {
        "key": ACCESS_KEY,
        "secret": SECRET_KEY,
        "client_kwargs": {
            "endpoint_url": lakefs_endpoint
        }
    }

    # Write DataFrame to a directory "output_parquet" partitioned by retrieval_time
    df.to_parquet(
    lakefs_s3_path,
    storage_options=storage_options,
    partition_cols=["acq_year","acq_month","acq_day","acq_hour","acq_minute"],   # <-- crucial for partitioning by retrieval_time
)

if __name__ == "__main__":
    apiweather_flow()