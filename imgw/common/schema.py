import pyarrow as pa
from dlt.common.schema import TTableSchemaColumns

### klimat
K_D_T_COLUMNS = {
    "station_code": pa.int64(),
    "station_name": pa.string(),
    "year": pa.int64(),
    "month": pa.int64(),
    "day": pa.int64(),
    "temp_daily_average": pa.float64(),
    "temp_average_status": pa.string(),
    "humidity_daily_average": pa.float64(),
    "humidity_status": pa.string(),
    "wind_speed_daily_average": pa.float64(),
    "wind_speed_status": pa.string(),
    "cloud_coverage_daily_average": pa.float64(),
    "cloud_coverage_status": pa.string(),
}

K_D_T_COLUMNS_DLT: TTableSchemaColumns = {
    "station_code": {"data_type": "bigint"},
    "station_name": {"data_type": "text"},
    "year": {"data_type": "bigint"},
    "month": {"data_type": "bigint"},
    "day": {"data_type": "bigint"},
    "temp_daily_average": {"data_type": "double"},
    "temp_daily_average_status": {"data_type": "text"},
    "humidity_daily_average": {"data_type": "double"},
    "humidity_daily_status": {"data_type": "text"},
    "wind_speed_daily_average": {"data_type": "double"},
    "wind_speed_daily_status": {"data_type": "text"},
    "cloud_coverage_daily_average": {"data_type": "double"},
    "cloud_coverage_daily_status": {"data_type": "text"},
}

K_D_COLUMNS = {
    "station_code": pa.int64(),
    "station_name": pa.string(),
    "year": pa.int64(),
    "month": pa.int64(),
    "day": pa.int64(),
    "temp_daily_max": pa.float64(),
    "temp_daily_max_status": pa.string(),
    "temp_daily_min": pa.float64(),
    "temp_daily_min_status": pa.string(),
    "temp_daily_average": pa.float64(),
    "temp_daily_average_status": pa.string(),
    "ground_temp_min": pa.float64(),
    "ground_temp_status": pa.string(),
    "precipitation_daily_total": pa.float64(),
    "precipitation_status": pa.string(),
    "precipitation_type": pa.string(),
    "snow_coverage_height": pa.float64(),
    "snow_coverage_status": pa.string(),
}

K_D_COLUMNS_DLT: TTableSchemaColumns = {
    "station_code": {"data_type": "bigint"},
    "station_name": {"data_type": "text"},
    "year": {"data_type": "bigint"},
    "month": {"data_type": "bigint"},
    "day": {"data_type": "bigint"},
    "temp_daily_max": {"data_type": "double"},
    "temp_max_status": {"data_type": "text"},
    "temp_daily_min": {"data_type": "double"},
    "temp_min_status": {"data_type": "text"},
    "temp_daily_average": {"data_type": "double"},
    "temp_average_status": {"data_type": "text"},
    "ground_temp_min": {"data_type": "double"},
    "ground_temp_status": {"data_type": "text"},
    "precipitation_daily_total": {"data_type": "double"},
    "precipitation_status": {"data_type": "text"},
    "precipitation_type": {"data_type": "text"},
    "snow_coverage_height": {"data_type": "double"},
    "snow_coverage_status": {"data_type": "text"},
}

### opady
O_D_COLUMNS = {
    "station_code": pa.int64(),
    "station_name": pa.string(),
    "year": pa.int64(),
    "month": pa.int64(),
    "day": pa.int64(),
    "precipitation_daily_total": pa.float64(),  # Suma dobowa opadów [mm]
    "precipitation_daily_total_status": pa.string(),  # Status pomiaru SMDB
    "precipitation_type": pa.string(),  # Rodzaj opadu [S/W/ ]
    "snow_cover_height": pa.float64(),  # Wysokość pokrywy śnieżnej [cm] - float safer
    "snow_cover_height_status": pa.string(),  # Status pomiaru PKSN
    "fresh_snow_height": pa.float64(),  # Wysokość świeżospałego śniegu [cm] - float safer
    "fresh_snow_height_status": pa.string(),  # Status pomiaru HSS
    "snow_type_code": pa.string(),  # Gatunek śniegu [kod]
    "snow_type_code_status": pa.string(),  # Status pomiaru GATS
    "snow_cover_type_code": pa.string(),  # Rodzaj pokrywy śnieżnej [kod] - length 5 suggests string code
    "snow_cover_type_code_status": pa.string(),  # Status pomiaru RPSN
}

O_D_COLUMNS_DLT: TTableSchemaColumns = {
    "station_code": {"data_type": "bigint"},
    "station_name": {"data_type": "text"},
    "year": {"data_type": "bigint"},
    "month": {"data_type": "bigint"},
    "day": {"data_type": "bigint"},
    "precipitation_daily_total": {"data_type": "double"},
    "precipitation_daily_total_status": {"data_type": "text"},
    "precipitation_type": {"data_type": "text"},  # Single character type
    "snow_cover_height": {"data_type": "double"},  # float safer
    "snow_cover_height_status": {"data_type": "text"},
    "fresh_snow_height": {"data_type": "double"},  # float safer
    "fresh_snow_height_status": {"data_type": "text"},
    "snow_type_code": {"data_type": "text"},  # Single character code
    "snow_type_code_status": {"data_type": "text"},
    "snow_cover_type_code": {"data_type": "text"},  # 5 character code
    "snow_cover_type_code_status": {"data_type": "text"},
}

S_D_COLUMNS = {
    "station_code": pa.int64(),
    "station_name": pa.string(),
    "year": pa.int64(),
    "month": pa.int64(),
    "day": pa.int64(),
    "temp_daily_max": pa.float64(),
    "temp_daily_max_status": pa.string(),
    "temp_daily_min": pa.float64(),
    "temp_daily_min_status": pa.string(),
    "temp_daily_average": pa.float64(),
    "temp_daily_average_status": pa.string(),
    "ground_temp_min": pa.float64(),
    "ground_temp_min_status": pa.string(),
    "precipitation_daily_total": pa.float64(),
    "precipitation_daily_total_status": pa.string(),  # Adjusted name based on SMDB
    "precipitation_type": pa.string(),
    "snow_coverage_height": pa.float64(),  # Assuming float is safer even if width is 5
    "snow_coverage_height_status": pa.string(),  # Adjusted name based on PKSN
    "snow_water_equivalent": pa.float64(),
    "snow_water_equivalent_status": pa.string(),
    "sunshine_duration": pa.float64(),
    "sunshine_duration_status": pa.string(),
    "duration_rain": pa.float64(),
    "duration_rain_status": pa.string(),
    "duration_snow": pa.float64(),
    "duration_snow_status": pa.string(),
    "duration_sleet": pa.float64(),  # Deszcz ze śniegiem = Sleet/Rain and snow
    "duration_sleet_status": pa.string(),
    "duration_hail": pa.float64(),
    "duration_hail_status": pa.string(),
    "duration_fog": pa.float64(),
    "duration_fog_status": pa.string(),
    "duration_mist": pa.float64(),  # Zamglenie = Mist
    "duration_mist_status": pa.string(),
    "duration_rime": pa.float64(),  # Sadź = Rime
    "duration_rime_status": pa.string(),
    "duration_glaze": pa.float64(),  # Gołoledź = Glaze ice
    "duration_glaze_status": pa.string(),
    "duration_low_drifting_snow": pa.float64(),  # Zamieć śnieżna niska = Low drifting snow
    "duration_low_drifting_snow_status": pa.string(),
    "duration_high_drifting_snow": pa.float64(),  # Zamieć śnieżna wysoka = High drifting snow
    "duration_high_drifting_snow_status": pa.string(),
    "duration_haze": pa.float64(),  # Zmętnienie = Haze/Turbidity
    "duration_haze_status": pa.string(),
    "duration_wind_gte_10mps": pa.float64(),  # Wiatr >= 10m/s
    "duration_wind_gte_10mps_status": pa.string(),
    "duration_wind_gt_15mps": pa.float64(),  # Wiatr > 15m/s
    "duration_wind_gt_15mps_status": pa.string(),
    "duration_thunderstorm": pa.float64(),  # Burza = Thunderstorm
    "duration_thunderstorm_status": pa.string(),
    "duration_dew": pa.float64(),  # Rosa = Dew
    "duration_dew_status": pa.string(),
    "duration_frost": pa.float64(),  # Szron = Frost
    "duration_frost_status": pa.string(),
    "occurrence_snow_cover": pa.int64(),  # Wystąpienie pokrywy śnieżnej [0/1]
    "occurrence_snow_cover_status": pa.string(),
    "occurrence_lightning": pa.int64(),  # Wystąpienie błyskawicy [0/1]
    "occurrence_lightning_status": pa.string(),
    "ground_state": pa.string(),  # Stan gruntu [Z/R] - Frozen/Thawed? Assuming string
    "isotherm_lower": pa.float64(),  # Izoterma dolna [cm]
    "isotherm_lower_status": pa.string(),
    "isotherm_upper": pa.float64(),  # Izoterma górna [cm]
    "isotherm_upper_status": pa.string(),
    "actinometry": pa.float64(),  # Aktynometria [J/cm2]
    "actinometry_status": pa.string(),
}

S_D_COLUMNS_DLT: TTableSchemaColumns = {
    "station_code": {"data_type": "bigint"},
    "station_name": {"data_type": "text"},
    "year": {"data_type": "bigint"},
    "month": {"data_type": "bigint"},
    "day": {"data_type": "bigint"},
    "temp_daily_max": {"data_type": "double"},
    "temp_daily_max_status": {"data_type": "text"},
    "temp_daily_min": {"data_type": "double"},
    "temp_daily_min_status": {"data_type": "text"},
    "temp_daily_average": {"data_type": "double"},
    "temp_daily_average_status": {"data_type": "text"},
    "ground_temp_min": {"data_type": "double"},
    "ground_temp_min_status": {"data_type": "text"},
    "precipitation_daily_total": {"data_type": "double"},
    "precipitation_daily_total_status": {"data_type": "text"},  # Adjusted name based on SMDB
    "precipitation_type": {"data_type": "text"},
    "snow_coverage_height": {"data_type": "double"},  # Assuming float is safer even if width is 5
    "snow_coverage_height_status": {"data_type": "text"},  # Adjusted name based on PKSN
    "snow_water_equivalent": {"data_type": "double"},
    "snow_water_equivalent_status": {"data_type": "text"},
    "sunshine_duration": {"data_type": "double"},
    "sunshine_duration_status": {"data_type": "text"},
    "duration_rain": {"data_type": "double"},
    "duration_rain_status": {"data_type": "text"},
    "duration_snow": {"data_type": "double"},
    "duration_snow_status": {"data_type": "text"},
    "duration_sleet": {"data_type": "double"},
    "duration_sleet_status": {"data_type": "text"},
    "duration_hail": {"data_type": "double"},
    "duration_hail_status": {"data_type": "text"},
    "duration_fog": {"data_type": "double"},
    "duration_fog_status": {"data_type": "text"},
    "duration_mist": {"data_type": "double"},
    "duration_mist_status": {"data_type": "text"},
    "duration_rime": {"data_type": "double"},
    "duration_rime_status": {"data_type": "text"},
    "duration_glaze": {"data_type": "double"},
    "duration_glaze_status": {"data_type": "text"},
    "duration_low_drifting_snow": {"data_type": "double"},
    "duration_low_drifting_snow_status": {"data_type": "text"},
    "duration_high_drifting_snow": {"data_type": "double"},
    "duration_high_drifting_snow_status": {"data_type": "text"},
    "duration_haze": {"data_type": "double"},
    "duration_haze_status": {"data_type": "text"},
    "duration_wind_gte_10mps": {"data_type": "double"},
    "duration_wind_gte_10mps_status": {"data_type": "text"},
    "duration_wind_gt_15mps": {"data_type": "double"},
    "duration_wind_gt_15mps_status": {"data_type": "text"},
    "duration_thunderstorm": {"data_type": "double"},
    "duration_thunderstorm_status": {"data_type": "text"},
    "duration_dew": {"data_type": "double"},
    "duration_dew_status": {"data_type": "text"},
    "duration_frost": {"data_type": "double"},
    "duration_frost_status": {"data_type": "text"},
    "occurrence_snow_cover": {"data_type": "bigint"},
    "occurrence_snow_cover_status": {"data_type": "text"},
    "occurrence_lightning": {"data_type": "bigint"},
    "occurrence_lightning_status": {"data_type": "text"},
    "ground_state": {"data_type": "text"},
    "isotherm_lower": {"data_type": "double"},
    "isotherm_lower_status": {"data_type": "text"},
    "isotherm_upper": {"data_type": "double"},
    "isotherm_upper_status": {"data_type": "text"},
    "actinometry": {"data_type": "double"},
    "actinometry_status": {"data_type": "text"},
}

S_D_T_COLUMNS = {
    "station_code": pa.int64(),
    "station_name": pa.string(),
    "year": pa.int64(),
    "month": pa.int64(),
    "day": pa.int64(),
    "average_daily_cloud_cover": pa.float64(),  # Zachmurzenie ogólne [oktanty]
    "cloud_cover_status": pa.string(),  # Status pomiaru NOS
    "average_daily_wind_speed": pa.float64(),  # Prędkość wiatru [m/s]
    "wind_speed_status": pa.string(),  # Status pomiaru FWS
    "temp_daily_average": pa.float64(),  # Temperatura [°C] (Assuming same as previous schema's average)
    "temp_daily_average_status": pa.string(),  # Status pomiaru TEMP
    "average_daily_vapor_pressure": pa.float64(),  # Ciśnienie pary wodnej [hPa]
    "vapor_pressure_status": pa.string(),  # Status pomiaru CPW
    "average_daily_relative_humidity": pa.float64(),  # Wilgotność względna [%]
    "relative_humidity_status": pa.string(),  # Status pomiaru WLGS
    "average_daily_station_pressure": pa.float64(),  # Ciśnienie na poziomie stacji [hPa]
    "station_pressure_status": pa.string(),  # Status pomiaru PPPS
    "average_daily_sea_level_pressure": pa.float64(),  # Ciśnienie na poziomie morza [hPa]
    "sea_level_pressure_status": pa.string(),  # Status pomiaru PPPM
    "precipitation_sum_day": pa.float64(),  # Suma opadu dzień [mm]
    "precipitation_sum_day_status": pa.string(),  # Status pomiaru WODZ
    "precipitation_sum_night": pa.float64(),  # Suma opadu noc [mm]
    "precipitation_sum_night_status": pa.string(),  # Status pomiaru WONO
}

S_D_T_COLUMNS_DLT: TTableSchemaColumns = {
    "station_code": {"data_type": "bigint"},
    "station_name": {"data_type": "text"},
    "year": {"data_type": "bigint"},
    "month": {"data_type": "bigint"},
    "day": {"data_type": "bigint"},
    "average_daily_cloud_cover": {"data_type": "double"},
    "cloud_cover_status": {"data_type": "text"},
    "average_daily_wind_speed": {"data_type": "double"},
    "wind_speed_status": {"data_type": "text"},
    "temp_daily_average": {"data_type": "double"},
    "temp_daily_average_status": {"data_type": "text"},
    "average_daily_vapor_pressure": {"data_type": "double"},
    "vapor_pressure_status": {"data_type": "text"},
    "average_daily_relative_humidity": {"data_type": "double"},
    "relative_humidity_status": {"data_type": "text"},
    "average_daily_station_pressure": {"data_type": "double"},
    "station_pressure_status": {"data_type": "text"},
    "average_daily_sea_level_pressure": {"data_type": "double"},
    "sea_level_pressure_status": {"data_type": "text"},
    "precipitation_sum_day": {"data_type": "double"},
    "precipitation_sum_day_status": {"data_type": "text"},
    "precipitation_sum_night": {"data_type": "double"},
    "precipitation_sum_night_status": {"data_type": "text"},
}

K_T_COLUMNS = {
    "station_code": pa.int64(),
    "station_name": pa.string(),
    "year": pa.int64(),
    "month": pa.int64(),
    "day": pa.int64(),
    "hour": pa.int64(),  # Godzina
    "air_temperature": pa.float64(),  # Temperatura powietrza [°C]
    "air_temperature_status": pa.string(),  # Status pomiaru TEMP
    "wet_bulb_temperature": pa.float64(),  # Temperatura termometru zwilżonego [°C]
    "wet_bulb_temperature_status": pa.string(),  # Status pomiaru TTZW
    "ice_indicator": pa.string(),  # Wskaźnik lodu [L/W] - Assuming single character code
    "ventilation_indicator": pa.string(),  # Wskaźnik wentylacji [W/N] - Assuming single character code
    "relative_humidity": pa.float64(),  # Wilgotność względna [%] - Width 5 suggests potential decimals or 100.0
    "relative_humidity_status": pa.string(),  # Status pomiaru WLGW
    "wind_direction_code": pa.string(),  # Kod kierunku wiatru [kod] - Width 3, safer as string code
    "wind_direction_code_status": pa.string(),  # Status pomiaru DKDK
    "wind_speed": pa.float64(),  # Prędkość wiatru [m/s] - Width 5 allows for decimals
    "wind_speed_status": pa.string(),  # Status pomiaru FWR
    "cloud_cover_general": pa.float64(),  # Zachmurzenie ogólne [oktanty] - Oktants usually int, but float safer
    "cloud_cover_general_status": pa.string(),  # Status pomiaru ZOGK
    "visibility_code": pa.string(),  # Widzialność [kod] - Width 5 suggests a code, safer as string
    "visibility_code_status": pa.string(),  # Status pomiaru WID
}

K_T_COLUMNS_DLT: TTableSchemaColumns = {
    "station_code": {"data_type": "bigint"},
    "station_name": {"data_type": "text"},
    "year": {"data_type": "bigint"},
    "month": {"data_type": "bigint"},
    "day": {"data_type": "bigint"},
    "hour": {"data_type": "bigint"},
    "air_temperature": {"data_type": "double"},
    "air_temperature_status": {"data_type": "text"},
    "wet_bulb_temperature": {"data_type": "double"},
    "wet_bulb_temperature_status": {"data_type": "text"},
    "ice_indicator": {"data_type": "text"},
    "ventilation_indicator": {"data_type": "text"},
    "relative_humidity": {"data_type": "double"},
    "relative_humidity_status": {"data_type": "text"},
    "wind_direction_code": {"data_type": "text"},  # Using text for code
    "wind_direction_code_status": {"data_type": "text"},
    "wind_speed": {"data_type": "double"},
    "wind_speed_status": {"data_type": "text"},
    "cloud_cover_general": {"data_type": "double"},  # Using double for oktants
    "cloud_cover_general_status": {"data_type": "text"},
    "visibility_code": {"data_type": "text"},  # Using text for code
    "visibility_code_status": {"data_type": "text"},
}

S_T_COLUMNS = {
    "station_code": pa.int64(),
    "station_name": pa.string(),
    "year": pa.int64(),
    "month": pa.int64(),
    "day": pa.int64(),
    "hour": pa.int64(),
    "cloud_base_cl_cm_coded": pa.string(),  # Wysokość podstawy chmur CL CM szyfrowana [kod]
    "cloud_base_cl_cm_coded_status": pa.string(),  # Status pomiaru HPOD
    "cloud_base_lower_m": pa.float64(),  # Wysokość podstawy niższej [m]
    "cloud_base_lower_m_status": pa.string(),  # Status pomiaru HPON
    "cloud_base_upper_m": pa.float64(),  # Wysokość podstawy wyższej [m]
    "cloud_base_upper_m_status": pa.string(),  # Status pomiaru HPOW
    "cloud_base_text": pa.string(),  # Wysokość podstawy tekstowy [opis]
    "measurement_instrument_1_lower": pa.string(),  # Pomiar przyrzadem 1 (niższa) [P]
    "measurement_instrument_2_upper": pa.string(),  # Pomiar przyrzadem 2 (wyższa) [P]
    "visibility_code": pa.string(),  # Widzialność [kod]
    "visibility_code_status": pa.string(),  # Status pomiaru WID
    "visibility_operator_m": pa.float64(),  # Widzialność operatora [m]
    "visibility_operator_m_status": pa.string(),  # Status pomiaru WIDO
    "visibility_auto_m": pa.float64(),  # Widzialność automat [m]
    "visibility_auto_m_status": pa.string(),  # Status pomiaru WIDA
    "cloud_cover_general_oktants": pa.string(),  # Zachmurzenie ogólne [oktanty]
    "cloud_cover_general_oktants_status": pa.string(),  # Status pomiaru NOG
    "wind_direction_deg": pa.float64(),  # Kierunek wiatru [°]
    "wind_direction_deg_status": pa.string(),  # Status pomiaru KRWR
    "wind_speed_ms": pa.float64(),  # Prędkość wiatru [m/s]
    "wind_speed_ms_status": pa.string(),  # Status pomiaru FWR
    "wind_gust_ms": pa.float64(),  # Poryw wiatru [m/s]
    "wind_gust_ms_status": pa.string(),  # Status pomiaru PORW
    "air_temperature_c": pa.float64(),  # Temperatura powietrza [°C]
    "air_temperature_c_status": pa.string(),  # Status pomiaru TEMP
    "wet_bulb_temperature_c": pa.float64(),  # Temperatura termometru zwilżonego [°C]
    "wet_bulb_temperature_c_status": pa.string(),  # Status pomiaru TTZW
    "ventilation_indicator": pa.string(),  # Wskaźnik wentylacji [W/N]
    "ice_indicator": pa.string(),  # Wskaźnik lodu [L/W]
    "vapor_pressure_hpa": pa.float64(),  # Ciśnienie pary wodnej [hPa]
    "vapor_pressure_hpa_status": pa.string(),  # Status pomiaru CPW
    "relative_humidity_percent": pa.float64(),  # Wilgotność względna [%]
    "relative_humidity_percent_status": pa.string(),  # Status pomiaru WLGW
    "dew_point_temperature_c": pa.float64(),  # Temperatura punktu rosy [°C]
    "dew_point_temperature_c_status": pa.string(),  # Status pomiaru TPTR
    "station_pressure_hpa": pa.float64(),  # Ciśnienie na pozimie stacji [hPa]
    "station_pressure_hpa_status": pa.string(),  # Status pomiaru PPPS
    "sea_level_pressure_hpa": pa.float64(),  # Ciśnienie na pozimie morza [hPa]
    "sea_level_pressure_hpa_status": pa.string(),  # Status pomiaru PPPM
    "pressure_tendency_characteristic_code": pa.string(),  # Charakterystyka tendencji [kod]
    "pressure_tendency_value": pa.float64(),  # Wartość tendencji [wartość]
    "pressure_tendency_status": pa.string(),  # Status pomiaru APP
    "precipitation_6hr_mm": pa.float64(),  # Opad za 6 godzin [mm]
    "precipitation_6hr_mm_status": pa.string(),  # Status pomiaru WO6G
    "precipitation_type_6hr_code": pa.string(),  # Rodzaj opadu za 6 godzin [kod]
    "precipitation_type_6hr_code_status": pa.string(),  # Status pomiaru ROPT
    "weather_present_code": pa.string(),  # Pogoda bieżąca [kod]
    "weather_past_code": pa.string(),  # Pogoda ubiegła [kod]
    "cloud_cover_low_oktants": pa.string(),  # Zachmurzenie niskie [oktanty]
    "cloud_cover_low_oktants_status": pa.string(),  # Status pomiaru CLCM
    "cloud_type_cl_code": pa.string(),  # Chmury CL [kod]
    "cloud_type_cl_code_status": pa.string(),  # Status pomiaru CHCL
    "cloud_type_cl_text": pa.string(),  # Chmury CL tekstem
    "cloud_type_cm_code": pa.string(),  # Chmury CM [kod]
    "cloud_type_cm_code_status": pa.string(),  # Status pomiaru CHCM
    "cloud_type_cm_text": pa.string(),  # Chmury CM tekstem
    "cloud_type_ch_code": pa.string(),  # Chmury CH [kod]
    "cloud_type_ch_code_status": pa.string(),  # Status pomiaru CHCH
    "cloud_type_ch_text": pa.string(),  # Chmury CH tekstem
    "ground_state_code": pa.string(),  # Stan gruntu [kod]
    "ground_state_code_status": pa.string(),  # Status pomiaru SGRN
    "humidity_deficit_hpa": pa.float64(),  # Niedosyt wilgotności [hPa]
    "humidity_deficit_hpa_status": pa.string(),  # Status pomiaru DEFI
    "sunshine_duration": pa.float64(),  # Usłonecznienie
    "sunshine_duration_status": pa.string(),  # Status pomiaru USLN
    "dew_occurrence_flag": pa.int64(),  # Wystąpienie rosy [0/1]
    "dew_occurrence_flag_status": pa.string(),  # Status pomiaru ROSW
    "max_gust_ww_period_ms": pa.float64(),  # Poryw maksymalny za okres WW [m/s]
    "max_gust_ww_period_ms_status": pa.string(),  # Status pomiaru PORK
    "max_gust_hour": pa.int64(),  # Godzina wystąpienia porywu
    "max_gust_minute": pa.int64(),  # Minuta wystąpienia porywu
    "ground_temp_5cm_c": pa.float64(),  # Temperatura gruntu -5 [°C]
    "ground_temp_5cm_c_status": pa.string(),  # Status pomiaru TG05
    "ground_temp_10cm_c": pa.float64(),  # Temperatura gruntu -10 [°C]
    "ground_temp_10cm_c_status": pa.string(),  # Status pomiaru TG10
    "ground_temp_20cm_c": pa.float64(),  # Temperatura gruntu -20 [°C]
    "ground_temp_20cm_c_status": pa.string(),  # Status pomiaru TG20
    "ground_temp_50cm_c": pa.float64(),  # Temperatura gruntu -50 [°C]
    "ground_temp_50cm_c_status": pa.string(),  # Status pomiaru TG50
    "ground_temp_100cm_c": pa.float64(),  # Temperatura gruntu -100 [°C]
    "ground_temp_100cm_c_status": pa.string(),  # Status pomiaru TG100
    "temp_min_12hr_c": pa.float64(),  # Temperatura minimalna za 12 godzin [°C]
    "temp_min_12hr_c_status": pa.string(),  # Status pomiaru TMIN
    "temp_max_12hr_c": pa.float64(),  # Temperatura maksymalna za 12 godzin [°C]
    "temp_max_12hr_c_status": pa.string(),  # Status pomiaru TMAX
    "ground_temp_min_12hr_c": pa.float64(),  # Temperatura minimalna przy gruncie za 12 godzin [°C]
    "ground_temp_min_12hr_c_status": pa.string(),  # Status pomiaru TGMI
    "snow_water_equivalent_mm_cm": pa.float64(),  # Równoważnik wodny śniegu [mm/cm]
    "snow_water_equivalent_mm_cm_status": pa.string(),  # Status pomiaru RWSN
    "snow_cover_height_cm": pa.float64(),  # Wysokość pokrywy śnieżnej [cm]
    "snow_cover_height_cm_status": pa.string(),  # Status pomiaru PKSN
    "fresh_snow_height_cm": pa.float64(),  # Wysokość świeżo spadłego śniegu [cm]
    "fresh_snow_height_cm_status": pa.string(),  # Status pomiaru HSS
    "snow_height_plot_cm": pa.float64(),  # Wysokość śniegu na poletku [cm]
    "snow_height_plot_cm_status": pa.string(),  # Status pomiaru GRSN
    "snow_type_code": pa.string(),  # Gatunek śniegu [kod]
    "snow_cover_formation_code": pa.string(),  # Ukształtowanie pokrywy [kod]
    "snow_sample_height_cm": pa.float64(),  # Wysokość próbki [cm]
    "snow_sample_height_cm_status": pa.string(),  # Status pomiaru HPRO
    "snow_water_storage_mm": pa.float64(),  # Zapas wody w śniegu [mm]
    "snow_water_storage_mm_status": pa.string(),  # Status pomiaru CIPR
}

S_T_COLUMNS_DLT: TTableSchemaColumns = {
    "station_code": {"data_type": "bigint"},
    "station_name": {"data_type": "text"},
    "year": {"data_type": "bigint"},
    "month": {"data_type": "bigint"},
    "day": {"data_type": "bigint"},
    "hour": {"data_type": "bigint"},
    "cloud_base_cl_cm_coded": {"data_type": "text"},
    "cloud_base_cl_cm_coded_status": {"data_type": "text"},
    "cloud_base_lower_m": {"data_type": "double"},
    "cloud_base_lower_m_status": {"data_type": "text"},
    "cloud_base_upper_m": {"data_type": "double"},
    "cloud_base_upper_m_status": {"data_type": "text"},
    "cloud_base_text": {"data_type": "text"},
    "measurement_instrument_1_lower": {"data_type": "text"},
    "measurement_instrument_2_upper": {"data_type": "text"},
    "visibility_code": {"data_type": "text"},
    "visibility_code_status": {"data_type": "text"},
    "visibility_operator_m": {"data_type": "double"},
    "visibility_operator_m_status": {"data_type": "text"},
    "visibility_auto_m": {"data_type": "double"},
    "visibility_auto_m_status": {"data_type": "text"},
    "cloud_cover_general_oktants": {"data_type": "text"},  # Oktants as integer
    "cloud_cover_general_oktants_status": {"data_type": "text"},
    "wind_direction_deg": {"data_type": "double"},
    "wind_direction_deg_status": {"data_type": "text"},
    "wind_speed_ms": {"data_type": "double"},
    "wind_speed_ms_status": {"data_type": "text"},
    "wind_gust_ms": {"data_type": "double"},
    "wind_gust_ms_status": {"data_type": "text"},
    "air_temperature_c": {"data_type": "double"},
    "air_temperature_c_status": {"data_type": "text"},
    "wet_bulb_temperature_c": {"data_type": "double"},
    "wet_bulb_temperature_c_status": {"data_type": "text"},
    "ventilation_indicator": {"data_type": "text"},
    "ice_indicator": {"data_type": "text"},
    "vapor_pressure_hpa": {"data_type": "double"},
    "vapor_pressure_hpa_status": {"data_type": "text"},
    "relative_humidity_percent": {"data_type": "double"},
    "relative_humidity_percent_status": {"data_type": "text"},
    "dew_point_temperature_c": {"data_type": "double"},
    "dew_point_temperature_c_status": {"data_type": "text"},
    "station_pressure_hpa": {"data_type": "double"},
    "station_pressure_hpa_status": {"data_type": "text"},
    "sea_level_pressure_hpa": {"data_type": "double"},
    "sea_level_pressure_hpa_status": {"data_type": "text"},
    "pressure_tendency_characteristic_code": {"data_type": "text"},
    "pressure_tendency_value": {"data_type": "double"},
    "pressure_tendency_status": {"data_type": "text"},
    "precipitation_6hr_mm": {"data_type": "double"},
    "precipitation_6hr_mm_status": {"data_type": "text"},
    "precipitation_type_6hr_code": {"data_type": "text"},
    "precipitation_type_6hr_code_status": {"data_type": "text"},
    "weather_present_code": {"data_type": "text"},
    "weather_past_code": {"data_type": "text"},
    "cloud_cover_low_oktants": {"data_type": "text"},  # Oktants as integer
    "cloud_cover_low_oktants_status": {"data_type": "text"},
    "cloud_type_cl_code": {"data_type": "text"},
    "cloud_type_cl_code_status": {"data_type": "text"},
    "cloud_type_cl_text": {"data_type": "text"},
    "cloud_type_cm_code": {"data_type": "text"},
    "cloud_type_cm_code_status": {"data_type": "text"},
    "cloud_type_cm_text": {"data_type": "text"},
    "cloud_type_ch_code": {"data_type": "text"},
    "cloud_type_ch_code_status": {"data_type": "text"},
    "cloud_type_ch_text": {"data_type": "text"},
    "ground_state_code": {"data_type": "text"},
    "ground_state_code_status": {"data_type": "text"},
    "humidity_deficit_hpa": {"data_type": "double"},
    "humidity_deficit_hpa_status": {"data_type": "text"},
    "sunshine_duration": {"data_type": "double"},
    "sunshine_duration_status": {"data_type": "text"},
    "dew_occurrence_flag": {"data_type": "bigint"},  # 0/1 flag
    "dew_occurrence_flag_status": {"data_type": "text"},
    "max_gust_ww_period_ms": {"data_type": "double"},
    "max_gust_ww_period_ms_status": {"data_type": "text"},
    "max_gust_hour": {"data_type": "bigint"},
    "max_gust_minute": {"data_type": "bigint"},
    "ground_temp_5cm_c": {"data_type": "double"},
    "ground_temp_5cm_c_status": {"data_type": "text"},
    "ground_temp_10cm_c": {"data_type": "double"},
    "ground_temp_10cm_c_status": {"data_type": "text"},
    "ground_temp_20cm_c": {"data_type": "double"},
    "ground_temp_20cm_c_status": {"data_type": "text"},
    "ground_temp_50cm_c": {"data_type": "double"},
    "ground_temp_50cm_c_status": {"data_type": "text"},
    "ground_temp_100cm_c": {"data_type": "double"},
    "ground_temp_100cm_c_status": {"data_type": "text"},
    "temp_min_12hr_c": {"data_type": "double"},
    "temp_min_12hr_c_status": {"data_type": "text"},
    "temp_max_12hr_c": {"data_type": "double"},
    "temp_max_12hr_c_status": {"data_type": "text"},
    "ground_temp_min_12hr_c": {"data_type": "double"},
    "ground_temp_min_12hr_c_status": {"data_type": "text"},
    "snow_water_equivalent_mm_cm": {"data_type": "double"},
    "snow_water_equivalent_mm_cm_status": {"data_type": "text"},
    "snow_cover_height_cm": {"data_type": "double"},
    "snow_cover_height_cm_status": {"data_type": "text"},
    "fresh_snow_height_cm": {"data_type": "double"},
    "fresh_snow_height_cm_status": {"data_type": "text"},
    "snow_height_plot_cm": {"data_type": "double"},
    "snow_height_plot_cm_status": {"data_type": "text"},
    "snow_type_code": {"data_type": "text"},
    "snow_cover_formation_code": {"data_type": "text"},
    "snow_sample_height_cm": {"data_type": "double"},
    "snow_sample_height_cm_status": {"data_type": "text"},
    "snow_water_storage_mm": {"data_type": "double"},
    "snow_water_storage_mm_status": {"data_type": "text"},
}

ARROW_COLUMNS_SCHEMA = {
    "k_d_t": K_D_T_COLUMNS,
    "k_d": K_D_COLUMNS,
    "o_d": O_D_COLUMNS,
    "s_d": S_D_COLUMNS,
    "s_d_t": S_D_T_COLUMNS,
    "k_t": K_T_COLUMNS,
    "s_t": S_T_COLUMNS,
}

DLT_COLUMNS_SCHEMA = {
    "k_d_t": K_D_T_COLUMNS_DLT,
    "k_d": K_D_COLUMNS_DLT,
    "o_d": O_D_COLUMNS_DLT,
    "s_d": S_D_COLUMNS_DLT,
    "s_d_t": S_D_T_COLUMNS_DLT,
    "k_t": K_T_COLUMNS_DLT,
    "s_t": S_T_COLUMNS_DLT,
}
