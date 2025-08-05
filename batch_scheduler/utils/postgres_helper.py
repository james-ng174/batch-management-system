from config import Config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from threading import Lock

class PostgresClient:
    _engine = None
    _lock = Lock()

    def __init__(self, logger):
        self.logger = logger
        # Ensure the engine is initialized only once
        with PostgresClient._lock:
            if not PostgresClient._engine:
                PostgresClient._engine = create_engine(Config.SQLALCHEMY_DATABASE_URI, echo=True)

        self.Session = sessionmaker(bind=PostgresClient._engine)

    def get_session(self):
        """
        Get a new session from the sessionmaker.
        """
        return self.Session()

    def get_records(self, entity, params, mapping):
        self.logger.info(f'============ Get db entity {entity.__name__}')
        session = self.get_session()

        try:
            results = session.query(entity).filter_by(**params).all()

            formatted_result = []
            for result in results:
                formatted_result.append(mapping(result))

        except Exception as e:
            session.close()
            self.logger.error(f'============ Get db entity throws exception: {e}')
            raise Exception(f'Exception get_records: {str(e.args)}')

        session.close()
        return {
            'success': True,
            'error_msg': None,
            'data': formatted_result
        }

    def create_record(self, entity, params):
        self.logger.info(f'============ Create db entity {entity.__name__}')
        self.logger.debug(f'Create params {params}')
        session = self.get_session()

        try:
            new_obj = entity(params)
            session.add(new_obj)
            session.commit()

        except Exception as e:
            session.rollback()
            session.close()
            raise Exception(f'Exception create_record: {str(e.args)}')

        session.close()
        return {
            'success': True,
            'error_msg': None,
            'data': None
        }

    def create_multiple_record(self, entity, obj_list):
        self.logger.info(f'============ Create multiple db entity {entity.__name__}')
        self.logger.debug(f'Create obj_list {obj_list}')
        session = self.get_session()

        try:
            new_obj_list = [entity(obj) for obj in obj_list]
            session.bulk_save_objects(new_obj_list)
            session.commit()

        except Exception as e:
            session.rollback()
            session.close()
            raise Exception(f'Exception create_record: {str(e.args)}')

        session.close()
        return {
            'success': True,
            'error_msg': None,
            'data': None
        }

    def update_record(self, entity, filter_params, params, check_record=True):
        self.logger.info(f'============ Update db entity {entity.__name__}')
        self.logger.debug(f'Update params {params}')
        session = self.get_session()

        try:
            if check_record:
                self.logger.info(f'============ Check record {entity.__name__}')
                check = session.query(entity).filter_by(**filter_params).all()
                if not check:
                    raise Exception('Record not existed')

            session.query(entity).filter_by(**filter_params).update(params)
            session.commit()

        except Exception as e:
            session.rollback()
            session.close()
            raise Exception(f'Exception update_record: {str(e.args)}')

        session.close()
        return {
            'success': True,
            'error_msg': None,
            'data': None
        }

    def delete_record(self, entity, filter_params):
        self.logger.info(f'============ Delete db entity {entity.__name__}')
        session = self.get_session()

        try:
            session.query(entity).filter_by(**filter_params).delete()
            session.commit()

        except Exception as e:
            session.rollback()
            session.close()
            raise Exception(f'Exception delete_record: {str(e.args)}')

        session.close()
        return {
            'success': True,
            'error_msg': None,
            'data': None
        }
