from sqlalchemy.orm import Session
from app.models.user import User
from app.schemas.user import UserCreate, UserUpdate
from app.core.logger import logger
from app.core.exceptions import NotFoundException, DatabaseException
from typing import List, Optional


class UserService:
    @staticmethod
    def get_user(db: Session, user_id: int) -> Optional[User]:
        try:
            user = db.query(User).filter(User.id == user_id).first()
            if not user:
                raise NotFoundException(f"User with id {user_id} not found")
            return user
        except Exception as e:
            logger.error(f"Error getting user: {e}")
            raise DatabaseException(f"Failed to get user: {e}")
    
    @staticmethod
    def get_user_by_email(db: Session, email: str) -> Optional[User]:
        try:
            return db.query(User).filter(User.email == email).first()
        except Exception as e:
            logger.error(f"Error getting user by email: {e}")
            raise DatabaseException(f"Failed to get user by email: {e}")
    
    @staticmethod
    def get_users(db: Session, skip: int = 0, limit: int = 100) -> List[User]:
        try:
            return db.query(User).offset(skip).limit(limit).all()
        except Exception as e:
            logger.error(f"Error getting users: {e}")
            raise DatabaseException(f"Failed to get users: {e}")
    
    @staticmethod
    def create_user(db: Session, user: UserCreate) -> User:
        try:
            # In a real application, you would hash the password here
            db_user = User(
                email=user.email,
                username=user.username,
                hashed_password=user.password  # This should be hashed
            )
            db.add(db_user)
            db.commit()
            db.refresh(db_user)
            logger.info(f"User created: {user.email}")
            return db_user
        except Exception as e:
            db.rollback()
            logger.error(f"Error creating user: {e}")
            raise DatabaseException(f"Failed to create user: {e}")
    
    @staticmethod
    def update_user(db: Session, user_id: int, user_update: UserUpdate) -> User:
        try:
            db_user = UserService.get_user(db, user_id)
            update_data = user_update.dict(exclude_unset=True)
            for field, value in update_data.items():
                setattr(db_user, field, value)
            db.commit()
            db.refresh(db_user)
            logger.info(f"User updated: {user_id}")
            return db_user
        except Exception as e:
            db.rollback()
            logger.error(f"Error updating user: {e}")
            raise DatabaseException(f"Failed to update user: {e}")
    
    @staticmethod
    def delete_user(db: Session, user_id: int) -> bool:
        try:
            db_user = UserService.get_user(db, user_id)
            db.delete(db_user)
            db.commit()
            logger.info(f"User deleted: {user_id}")
            return True
        except Exception as e:
            db.rollback()
            logger.error(f"Error deleting user: {e}")
            raise DatabaseException(f"Failed to delete user: {e}")
