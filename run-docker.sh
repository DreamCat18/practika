docker run -it --rm \
  -v "$(pwd):/app" \
  -e DISPLAY=$DISPLAY \
  -v /tmp/.X11-unix:/tmp/.X11-unix \
  -u $(id -u):$(id -g) \
  --name cms-app \
  python:3.9-slim \
  sh -c "
    apt-get update && apt-get install -y tk tcl python3-pip && \
    pip install pandas matplotlib seaborn numpy openpyxl requests && \
    cd /app && python client_management_system.py
  "