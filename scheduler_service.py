from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from config.settings import DB_CONFIG

jobstores = {
    'default': SQLAlchemyJobStore(url=f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}")
}

scheduler = BackgroundScheduler(jobstores=jobstores)

def init_scheduler():
    if not scheduler.running:
        scheduler.start()
