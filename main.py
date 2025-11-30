import time
import instaloader
from neo4j import GraphDatabase
import os

USER = os.getenv("USER")
PASSWD = os.getenv("PASSWD")
NEO4_USER = os.getenv("NEO4_USER")
NEO4_URL = os.getenv("NEO4_URL")
NEO4_PASSWD = os.getenv("NEO4_PASSWD")

def create_user1(tx, userid:str):
    tx.run("""
    MERGE (u:User {id: $userid})
    SET u.dt = datetime()
    """, userid=userid)

def create_user2(tx, userid:str):
    tx.run("""
    MERGE (u:User {id: $userid})
    ON CREATE
        SET u.dt = 0
    """, userid=userid)

def create_relationship(tx, user1:str, user2:str):
    tx.run("""
        MATCH (a:User {id: $u1})
        MATCH (b:User {id: $u2})
        MERGE (a)-[rel:FOLLOWS]->(b)
        SET rel.dt = datetime()
    """, u1=user1, u2=user2)

def get_todo(tx):
    result = tx.run("MATCH (a:User) ORDER BY a.dt LIMIT 100 RETURN a.id")
    return set([record["a.id"] for record in result])

def add(session, user: str, followers: set, followees: set):
    session.execute_write(create_user1, user)

    for follower in followers:
        session.execute_write(create_user2, follower)
        session.execute_write(create_relationship, follower, user)

    for followee in followees:
        session.execute_write(create_user2, followee)
        session.execute_write(create_relationship, user, followee)

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
        i = 0        
        L = instaloader.Instaloader()
        L.load_session_from_file(USER)
        to_parcours = session.execute_read(get_todo)

        if (len(to_parcours) == 0):
            x = instaloader.Profile.from_username(L.context, USER)
            to_parcours.add(x.userid)

        while len(to_parcours) > 0:
            profileid = to_parcours.pop()
            profile = instaloader.Profile.from_id(L.context, profileid)

            if is_viewable(profile):
                followers = set(x.userid for x in profile.get_followers()) if profile.followers < 5000 else set()
                followees = set(x.userid for x in profile.get_followees()) if profile.followees < 5000 else set()
            else:
                followers,followees = set(),set()
            
            add(session,profileid,followers,followees)

            i = (i + 1)%2
            if (len(to_parcours) == 0):
                to_parcours = session.execute_read(get_todo)
            if i == 0:
                time.sleep(340)

if __name__ == "__main__":
    main()
