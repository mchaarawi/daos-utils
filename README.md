# daos-utils
DAOS Utilities for management of Aurora's DAOS storage system

daos-pool-balancer.py
- script to generate a dmg pool create command with specific ranks

daos-metrics.py
- retrieves and reports on telemetry from DAOS servers
- python3.10 daos-metrics.py --config config.json --interval 0 --ranks 0 --metric engine_pool_xferred_update

kafka-metrics.py
- retrieves and reports on telemetry from Kafka
- python3.10 kafka-metrics.py --config=./kafka.config --topic <topic> --metric=engine_pool_ops_cont_open_counter --latest
