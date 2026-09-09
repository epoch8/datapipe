PROTOS_DIR=./datapipe_router/protos
PB2_DIR=./datapipe_router/pb2

generate_agent_messages:
	python -m grpc_tools.protoc \
	-I ${PROTOS_DIR} \
	--python_out=${PB2_DIR} \
	--pyi_out=${PB2_DIR} \
	--grpc_python_out=${PB2_DIR} \
	agent.proto

generate_client_messages:
	python -m grpc_tools.protoc \
	-I ${PROTOS_DIR} \
	--python_out=${PB2_DIR} \
	--pyi_out=${PB2_DIR} \
	--grpc_python_out=${PB2_DIR} \
	client.proto