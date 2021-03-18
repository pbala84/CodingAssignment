# CodingAssignment

  Coding Assignment as given in READMEAssignment.md completed 
  
  Data is pulled from 3 endpoints, stored in MONGODB and a REST API is provided with required aggregations
  
  After pulling the project, steps to run the app (when docker-compose is installed), execute the following cmds
   - docker-compose build
   - docker-compose up
   
  When docker containers are up and running
   - http://127.0.0.1:80/ -> will say Hello!!
   - REST API result can be seen at
      - http://127.0.0.1:80/excavator_operating_hours_since_last_maintenance
      - http://127.0.0.1:80/excavator_operational
      - http://127.0.0.1:80/excavator_last_10_CAN_messages
      - http://127.0.0.1:80/excavator_average_fuel_rate_past_24h
