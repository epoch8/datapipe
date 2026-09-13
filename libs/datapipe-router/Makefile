PROTOS_DIR=./datapipe_router/protos
PB2_DIR=./datapipe_router/pb2

generate_storage_agent_messages:
	python -m grpc_tools.protoc \
	-I ${PROTOS_DIR} \
	--python_out=${PB2_DIR} \
	--pyi_out=${PB2_DIR} \
	--grpc_python_out=${PB2_DIR} \
	storage_agent.proto

generate_storage_client_messages:
	python -m grpc_tools.protoc \
	-I ${PROTOS_DIR} \
	--python_out=${PB2_DIR} \
	--pyi_out=${PB2_DIR} \
	--grpc_python_out=${PB2_DIR} \
	storage_client.proto

generate_router_agent_messages:
	python -m grpc_tools.protoc \
	-I ${PROTOS_DIR} \
	--python_out=${PB2_DIR} \
	--pyi_out=${PB2_DIR} \
	--grpc_python_out=${PB2_DIR} \
	router_agent.proto

generate_router_client_messages:
	python -m grpc_tools.protoc \
	-I ${PROTOS_DIR} \
	--python_out=${PB2_DIR} \
	--pyi_out=${PB2_DIR} \
	--grpc_python_out=${PB2_DIR} \
	router_client.proto

generate_storage_messages: generate_storage_agent_messages generate_storage_client_messages 
generate_router_messages: generate_router_agent_messages generate_router_client_messages
generate_all: generate_storage_messages generate_router_messages