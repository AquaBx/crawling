import time
import instaloader
from neo4j import GraphDatabase
import os
import requests
from PIL import Image
from io import BytesIO
import logging
import sys

logger = logging.getLogger(__name__) 
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
console_handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(console_handler)

def download_pp(url:str, dir:str):
    reponse = requests.get(url)
    reponse.raise_for_status()
    image_bytes = BytesIO(reponse.content)
    image_pil = Image.open(image_bytes)

    image_pil.save( dir,  format="AVIF" )

USER = os.getenv("USER")
PASSWD = os.getenv("PASSWD")
NEO4_USER = os.getenv("NEO4_USER")
NEO4_URL = os.getenv("NEO4_URL")
NEO4_PASSWD = os.getenv("NEO4_PASSWD")
OUTDIR = os.getenv("OUTDIR")

def create_user1(tx, userid:int, username:str):
    tx.run("""
    MERGE (u:User {id: $userid})
    SET u.dt = datetime(),u.username = $username
    """, userid=userid, username=username)

def create_user2(tx, userid:int, username:str):
    tx.run("""
    MERGE (u:User {id: $userid})
    ON CREATE
        SET u.username = $username , u.dt = datetime({epochmillis: 0})
    ON MATCH
        SET u.username = $username
    """, userid=userid, username=username)

def create_relationship(tx, user1:int, user2:int):
    tx.run("""
        MATCH (a:User {id: $u1})
        MATCH (b:User {id: $u2})
        MERGE (a)-[rel:FOLLOWS]->(b)
        SET rel.dt = datetime()
    """, u1=user1, u2=user2)

def get_todo(tx):
    result = tx.run("MATCH (a:User) ORDER BY a.dt LIMIT 25 RETURN a.id")
    return set([record["a.id"] for record in result])

def is_viewable(profile):
    if profile.followers > 10000:
        return False
    elif not(profile.is_private):
        return True
    elif profile.followed_by_viewer:
        return True
    elif USER == profile.username: 
        return True
    return False

def main():
    driver = GraphDatabase.driver(NEO4_URL, auth=(NEO4_USER, NEO4_PASSWD))
    with driver.session() as session:
        L = instaloader.Instaloader()
        L.load_session_from_file(USER)
        to_parcours = session.execute_read(get_todo)

        if (len(to_parcours) == 0):
            x = instaloader.Profile.from_username(L.context, USER)
            to_parcours.add(x.userid)
        
        while len(to_parcours) > 0:
            profileid = to_parcours.pop()
            logger.info("New Profil")
            profile = instaloader.Profile.from_id(L.context, profileid)

            logger.info("Download Profil")
            session.execute_write(create_user1, profileid, profile.username)
            logger.info("Download PP")
            download_pp(profile.profile_pic_url, "{}/{}.avif".format(OUTDIR,profileid))

            if is_viewable(profile):
                y = 0
                logger.info("Download Followers")

                for follower in profile.get_followers():
                    session.execute_write(create_user2, follower.userid, follower.username)
                    session.execute_write(create_relationship, follower.userid, profileid)
                    y+=1
                    if y > 1000:
                        break
                logger.info("Download Followees")
                y = 0
                for followee in profile.get_followees():
                    session.execute_write(create_user2, followee.userid, followee.username)
                    session.execute_write(create_relationship, profileid, followee.userid)
                    y+=1
                    if y > 1000:
                        break
                time.sleep(180)
            else : 
                time.sleep(10)

            if (len(to_parcours) == 0):
                logger.info("Fetching new profiles")
                to_parcours = session.execute_read(get_todo)

if __name__ == "__main__":
    logger.info("Start Application")
    try:
        main()
    except Exception as e: 
        logger.error(e)
        logger.error("Will sleep undefinitely")

        while True:
            time.sleep(10)
