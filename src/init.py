from logger import logging
import sys
from exception import CustomException
from ingestion.data_ingestion import run_data_ingestion
from datetime import datetime

if __name__=="__main__":
    timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    logging.info("The execution has started")
    try:
        data_ingestion=run_data_ingestion(timestamp)





        # train_data_path,test_data_path=data_ingestion.initiate_data_ingestion()

        # #data_transformation_config=DataTransformationConfig()
        # data_transformation=DataTransformation()
        # train_arr,test_arr,_=data_transformation.initiate_data_transormation(train_data_path,test_data_path)

        # ## Model Training

        # model_trainer=ModelTrainer()
        # print(model_trainer.initiate_model_trainer(train_arr,test_arr))
        
    except Exception as e:
        logging.info(f"Custom Exception {e}")
        raise CustomException(e,sys)