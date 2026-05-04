import random
import psycopg2
from psycopg2 import sql, extras
from faker import Faker
from typing import Dict, List, Any, Set
from datetime import datetime, timedelta
import os
import sys

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'astronomy_db',
    'user': 'postgres',
    'password': 'ChgSQL(0201)'
}

fake = Faker(['en_US'])
Faker.seed(42)
random.seed(42)

OBJECT_TYPES = ["Galaxy", "Nebula", "Star", "Cluster", "Remnant", "Nebula", 
                "Quasar", "Pulsar", "BlackHole", "Dwarf", "Giant", "WDwarf"]

EDU_TYPES = ["University", "College", "Institute", "Academy", "School", "Research"]
RESEARCH_TYPES = ["State", "Private", "Mixed", "International", "Non-profit", "Commercial"]
TELESCOPE_TYPES = ["Optical", "Radio", "Infrared", "X-ray", "Gamma", "UV", "Solar"]
TELESCOPE_SPOTS = ["Space", "Hawaii", "Chile", "Canary", "Australia", "S.Africa", 
                   "Arizona", "PuertoRico", "China", "India", "Namibia", "Argentina"]

PROFESSIONS = [
    "Astrophysics", "Cosmology", "PlanetarySci", "StellarAstro", "RadioAstro",
    "SolarPhysics", "GalacticAstro", "Extragalactic", "Astrometry", 
    "Exoplanetology", "Heliophysics", "Astrochemistry"
]

GRADUATES = [
    "PhD", "Master", "Bachelor", "Professor", "Dr.Science", "Cand.Science",
    "Sr.Researcher", "Lead.Researcher", "Assoc.Prof", "Postdoc"
]

ORGANISATION_PREFIXES = ["Intl", "National", "European", "Asian", "African", "American",
                          "Russian", "German", "French", "Japanese", "Chinese", "Indian"]

ORGANISATION_SUFFIXES = ["SpaceInst", "Observatory", "SpaceCenter", 
                         "AstroLab", "PlanetarySoc", "CosmicCenter", 
                         "RadioStn", "CosmicLab"]

OBJECT_NAMES = ["Andromeda", "MilkyWay", "OrionNeb", "Pleiades", "Betelgeuse", "Sirius", "Vega", 
                "CrabNeb", "BlackEye", "Sombrero", "Whirlpool", "Pinwheel",
                "Triangulum", "CatsEye", "RingNeb", "EagleNeb", "Horsehead",
                "Tarantula", "RoseGal", "Sunflower", "Tadpole", "CigarGal"]

CATALOG_NAMES = ["NGC", "IC", "Messier", "Caldwell", "PGC", "UGC", "SDSS", "2MASS", "HIP", "Tycho", 
                 "GaiaDR3", "WISE", "Hipparcos", "GSC", "USNO", "NOMAD", "DENIS", "IRAS"]

COUNTRIES = ["USA", "UK", "France", "Germany", "Japan", "Russia", "China", "Italy", "Canada", "Australia", 
             "Spain", "Netherlands", "Switzerland", "Sweden", "India", "Brazil", "Mexico", "S.Africa", 
             "S.Korea", "Israel", "Poland", "Ukraine", "Norway", "Finland", "Denmark", "Austria", "Belgium"]

FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", 
    "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa",
    "Matthew", "Betty", "Anthony", "Margaret", "Donald", "Sandra", "Mark", "Ashley",
    "Paul", "Kimberly", "Steven", "Emily", "Andrew", "Donna", "Kenneth", "Michelle",
    "George", "Carol", "Joshua", "Amanda", "Kevin", "Dorothy", "Brian", "Melissa",
    "Edward", "Deborah", "Ronald", "Stephanie", "Timothy", "Rebecca", "Jason", "Sharon",
    "Jeffrey", "Laura", "Ryan", "Cynthia", "Jacob", "Kathleen", "Gary", "Amy"
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Wilson", "Anderson", "Taylor", "Thomas", "Moore", "Jackson",
    "Martin", "Lee", "White", "Harris", "Clark", "Lewis", "Robinson", "Walker", "Hall",
    "Young", "Allen", "King", "Wright", "Scott", "Green", "Baker", "Adams", "Nelson",
    "Hill", "Ramirez", "Campbell", "Mitchell", "Roberts", "Carter", "Phillips", "Evans",
    "Turner", "Torres", "Parker", "Collins", "Edwards", "Stewart", "Flores", "Morris"
]

def generate_unique_names(count: int, existing_names: Set[str] = None) -> List[str]:
    if existing_names is None:
        existing_names = set()
    
    unique_names = []
    attempts = 0
    max_attempts = count * 10
    
    while len(unique_names) < count and attempts < max_attempts:
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        if count > 5000:
            name = f"{first} {last} {len(unique_names) + 1}"
        else:
            name = f"{first} {last}"
        
        if name not in existing_names:
            existing_names.add(name)
            unique_names.append(name)
        attempts += 1
    
    while len(unique_names) < count:
        name = f"Person_{len(unique_names) + 1}"
        if name not in existing_names:
            existing_names.add(name)
            unique_names.append(name)
    
    return unique_names

def create_connection():
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            database=DB_CONFIG['database'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            client_encoding='UTF8'
        )
        print(f"Connected to database '{DB_CONFIG['database']}'")
        return conn
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        print("\nCheck:")
        print("1. Is PostgreSQL server running?")
        print("2. Correct parameters in DB_CONFIG?")
        print("3. Does database 'astronomy_db' exist?")
        print("4. Correct password?")
        raise

def init_database():
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            database='postgres',
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            client_encoding='UTF8'
        )
        conn.autocommit = True
        cur = conn.cursor()
        
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_CONFIG['database'],))
        exists = cur.fetchone()
        
        if not exists:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DB_CONFIG['database'])))
            print(f"Database '{DB_CONFIG['database']}' created")
        else:
            print(f"Database '{DB_CONFIG['database']}' already exists")
        
        cur.close()
        conn.close()
        return True
    except psycopg2.Error as e:
        print(f"Could not check/create database: {e}")
        return False

def create_schema(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS Astronomy")
        cur.execute("SET search_path TO Astronomy")
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Catalog (
                ID_Catalog DECIMAL PRIMARY KEY,
                Count DECIMAL NOT NULL,
                CatalogName VARCHAR(100) NOT NULL
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Educational_institution (
                ID_Organisation DECIMAL PRIMARY KEY,
                Type VARCHAR(20) NOT NULL,
                Country VARCHAR(48) NOT NULL,
                Budget DECIMAL NOT NULL,
                Organisation VARCHAR(100) NOT NULL
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Research_organisation (
                ID_Organisation VARCHAR(100) PRIMARY KEY,
                Type VARCHAR(15) NOT NULL,
                Country VARCHAR(48) NOT NULL,
                Budget DECIMAL NOT NULL,
                Organisation VARCHAR(100) NOT NULL
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Scientist (
                Person VARCHAR(55) PRIMARY KEY,
                Country VARCHAR(48) NOT NULL,
                Proffesion VARCHAR(20) NOT NULL,
                Graduate VARCHAR(20) NOT NULL,
                ID_Organisation DECIMAL NOT NULL,
                ID_Catalog DECIMAL NOT NULL
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Amateur_astronomer (
                Person VARCHAR(55) PRIMARY KEY,
                Country VARCHAR(48) NOT NULL,
                Age VARCHAR(3) NOT NULL,
                ID_Organisation DECIMAL NOT NULL,
                ID_Catalog DECIMAL NOT NULL
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Automatic_telescope (
                Telescope VARCHAR(32) PRIMARY KEY,
                Type VARCHAR(32) NOT NULL,
                ID_Organisation VARCHAR(100) NOT NULL,
                Year DECIMAL NOT NULL,
                Spot VARCHAR(40) NOT NULL,
                ID_Catalog DECIMAL NOT NULL
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Object (
                ID_Catalog DECIMAL NOT NULL,
                Type VARCHAR(20) NOT NULL,
                Declension DECIMAL NOT NULL,
                Size DECIMAL NOT NULL,
                Magnitude DECIMAL NOT NULL,
                Object VARCHAR(30) NOT NULL
            )
        """)
        
        conn.commit()
        print("Schema and tables created")

def clear_tables(conn):
    with conn.cursor() as cur:
        cur.execute("SET search_path TO Astronomy")
        cur.execute("TRUNCATE TABLE Object CASCADE")
        cur.execute("TRUNCATE TABLE Automatic_telescope CASCADE")
        cur.execute("TRUNCATE TABLE Amateur_astronomer CASCADE")
        cur.execute("TRUNCATE TABLE Scientist CASCADE")
        cur.execute("TRUNCATE TABLE Research_organisation CASCADE")
        cur.execute("TRUNCATE TABLE Educational_institution CASCADE")
        cur.execute("TRUNCATE TABLE Catalog CASCADE")
        conn.commit()
        print("Tables cleared")

def generate_catalogs(conn, count: int):
    print(f"  Generating catalogs ({count} records)...")
    data = []
    for i in range(count):
        catalog = (i + 1, random.randint(10, 50000), f"{random.choice(CATALOG_NAMES)}-{random.randint(1, 999)}")
        data.append(catalog)
    
    with conn.cursor() as cur:
        extras.execute_values(cur, "INSERT INTO Catalog (ID_Catalog, Count, CatalogName) VALUES %s", data)
    conn.commit()

def generate_educational_institutions(conn, count: int):
    print(f"  Generating educational institutions ({count} records)...")
    data = []
    for i in range(count):
        edu_type = random.choice(EDU_TYPES)
        if len(edu_type) > 20:
            edu_type = edu_type[:20]
        
        institution = (i + 1, edu_type, random.choice(COUNTRIES), 
                      round(random.uniform(1e6, 1e11), 0), f"{fake.company()} Univ")
        data.append(institution)
    
    with conn.cursor() as cur:
        extras.execute_values(cur, "INSERT INTO Educational_institution (ID_Organisation, Type, Country, Budget, Organisation) VALUES %s", data)
    conn.commit()

def generate_research_organisations(conn, count: int):
    print(f"  Generating research organisations ({count} records)...")
    data = []
    for i in range(count):
        org_type = random.choice(RESEARCH_TYPES)
        if len(org_type) > 15:
            org_type = org_type[:15]
            
        organisation = (f"RO_{i+1:04d}", org_type, random.choice(COUNTRIES),
                       round(random.uniform(5e5, 5e10), 0), f"{random.choice(ORGANISATION_PREFIXES)} {random.choice(ORGANISATION_SUFFIXES)}")
        data.append(organisation)
    
    with conn.cursor() as cur:
        extras.execute_values(cur, "INSERT INTO Research_organisation (ID_Organisation, Type, Country, Budget, Organisation) VALUES %s", data)
    conn.commit()

def generate_scientists(conn, count: int, edu_ids: list, catalog_ids: list):
    print(f"  Generating scientists ({count} records)...")
    
    unique_names = generate_unique_names(count)
    
    data = []
    for i in range(count):
        country = random.choice(COUNTRIES)
        profession = random.choice(PROFESSIONS)
        graduate = random.choice(GRADUATES)
        
        scientist = (unique_names[i], country, profession, graduate,
                    random.choice(edu_ids), random.choice(catalog_ids))
        data.append(scientist)
    
    with conn.cursor() as cur:
        extras.execute_values(cur, "INSERT INTO Scientist (Person, Country, Proffesion, Graduate, ID_Organisation, ID_Catalog) VALUES %s", data, page_size=1000)
    conn.commit()
    print(f"    Generated {len(data)} unique scientists")

def generate_amateurs(conn, count: int, edu_ids: list, catalog_ids: list):
    print(f"  Generating amateur astronomers ({count} records)...")
    
    unique_names = generate_unique_names(count)
    
    data = []
    for i in range(count):
        amateur = (unique_names[i], random.choice(COUNTRIES), str(random.randint(16, 85)),
                  random.choice(edu_ids), random.choice(catalog_ids))
        data.append(amateur)
    
    with conn.cursor() as cur:
        extras.execute_values(cur, "INSERT INTO Amateur_astronomer (Person, Country, Age, ID_Organisation, ID_Catalog) VALUES %s", data, page_size=1000)
    conn.commit()
    print(f"    Generated {len(data)} unique amateur astronomers")

def generate_telescopes(conn, count: int, research_ids: list, catalog_ids: list):
    print(f"  Generating telescopes ({count} records)...")
    data = []
    telescope_names = ["Hubble", "Webb", "Keck", "VLT", "ALMA", "Chandra", "Fermi", "FAST", "Gemini", "Subaru"]
    
    used_names = set()
    for i in range(count):
        while True:
            tel_name = f"{random.choice(telescope_names)}-{random.randint(1, 9999)}"
            if tel_name not in used_names:
                used_names.add(tel_name)
                break
        
        tel_type = random.choice(TELESCOPE_TYPES)
        if len(tel_type) > 32:
            tel_type = tel_type[:32]
        spot = random.choice(TELESCOPE_SPOTS)
        if len(spot) > 40:
            spot = spot[:40]
            
        telescope = (tel_name, tel_type, random.choice(research_ids), 
                    random.randint(1950, 2025), spot, random.choice(catalog_ids))
        data.append(telescope)
    
    with conn.cursor() as cur:
        extras.execute_values(cur, "INSERT INTO Automatic_telescope (Telescope, Type, ID_Organisation, Year, Spot, ID_Catalog) VALUES %s", data)
    conn.commit()

def generate_objects(conn, count: int, catalog_ids: list):
    print(f"  Generating space objects ({count:,} records)...")
    data = []
    for _ in range(count):
        obj_type = random.choice(OBJECT_TYPES)
        obj_name = f"{random.choice(OBJECT_NAMES)}_{random.randint(1, 100000)}"
        if len(obj_name) > 30:
            obj_name = obj_name[:30]
            
        obj = (random.choice(catalog_ids), obj_type,
              round(random.uniform(-90, 90), 4), round(random.uniform(0.01, 120), 2),
              round(random.uniform(-30, 20), 2), obj_name)
        data.append(obj)
    
    with conn.cursor() as cur:
        extras.execute_values(cur, "INSERT INTO Object (ID_Catalog, Type, Declension, Size, Magnitude, Object) VALUES %s", data, page_size=5000)
    conn.commit()

def main():
    print("=" * 70)
    print("TEST DATA GENERATOR FOR ASTRONOMY DATABASE")
    print("=" * 70)
    
    counts = {
        "catalogs": 500,
        "edu_institutions": 500,
        "research_orgs": 200,
        "scientists": 5000,
        "amateurs": 10000,
        "telescopes": 500,
        "objects": 200000
    }
    
    try:
        print("\nInitializing database...")
        if not init_database():
            print("Failed to initialize database. Exiting.")
            return
        
        print("\nConnecting to PostgreSQL...")
        conn = create_connection()
        
        print("\nCreating database structure...")
        create_schema(conn)
        
        print("\nClearing existing data...")
        clear_tables(conn)
        
        print("\nGenerating data:\n")
        
        generate_catalogs(conn, counts["catalogs"])
        generate_educational_institutions(conn, counts["edu_institutions"])
        generate_research_organisations(conn, counts["research_orgs"])
        
        catalog_ids = list(range(1, counts["catalogs"] + 1))
        edu_ids = list(range(1, counts["edu_institutions"] + 1))
        research_ids = [f"RO_{i+1:04d}" for i in range(counts["research_orgs"])]
        
        generate_scientists(conn, counts["scientists"], edu_ids, catalog_ids)
        generate_amateurs(conn, counts["amateurs"], edu_ids, catalog_ids)
        generate_telescopes(conn, counts["telescopes"], research_ids, catalog_ids)
        generate_objects(conn, counts["objects"], catalog_ids)
        
        print("\n" + "=" * 70)
        print("DATA GENERATION COMPLETED SUCCESSFULLY!")
        print("=" * 70)
        print(f"\nStatistics:")
        print(f"  Catalogs: {counts['catalogs']:,}")
        print(f"  Educational institutions: {counts['edu_institutions']:,}")
        print(f"  Research organisations: {counts['research_orgs']:,}")
        print(f"  Scientists: {counts['scientists']:,}")
        print(f"  Amateur astronomers: {counts['amateurs']:,}")
        print(f"  Telescopes: {counts['telescopes']:,}")
        print(f"  Space objects: {counts['objects']:,}")
        print(f"  TOTAL: {sum(counts.values()):,}")
        
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("\nDatabase connection closed")

main()