import asyncio
import json
import logging
import websockets
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

# Global state
admins = set()
agent = None

async def handler(websocket):
    global agent
    
    # Wait for the first message to identify the role (admin or agent)
    try:
        registration_message = await websocket.recv()
        data = json.loads(registration_message)
        role = data.get("role")
    except Exception as e:
        logging.error(f"Failed to parse registration message: {e}")
        return

    if role == "admin":
        admins.add(websocket)
        logging.info(f"Admin connected. Total admins: {len(admins)}")
        
        # Tell the newly connected admin if the agent is already online
        if agent is not None:
            await websocket.send(json.dumps({"status": "agent_connected", "message": "Agent is ready"}))
            
        try:
            async for message in websocket:
                # Messages from admin should be JSON commands
                try:
                    command_data = json.loads(message)
                    command = command_data.get("command")
                    if command in ["start_camera", "stop_camera"]:
                        if agent is not None:
                            await agent.send(json.dumps({"command": command}))
                        else:
                            await websocket.send(json.dumps({"status": "error", "message": "Agent not connected"}))
                except json.JSONDecodeError:
                    pass
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            admins.remove(websocket)
            logging.info(f"Admin disconnected. Total admins: {len(admins)}")

    elif role == "agent":
        if agent is not None:
            logging.warning("An agent is already connected! Overwriting...")
            # Optionally close the old agent
            # await agent.close()
            
        agent = websocket
        logging.info("Agent connected.")
        
        # Notify all waiting admins that the agent is online
        for admin_ws in list(admins):
            try:
                await admin_ws.send(json.dumps({"status": "agent_connected", "message": "Agent is ready"}))
            except websockets.exceptions.ConnectionClosed:
                admins.remove(admin_ws)
                
        try:
            async for message in websocket:
                # Handle binary frames (video) or text
                if isinstance(message, bytes):
                    # Broadcast video frames to all connected admins
                    for admin_ws in list(admins):
                        try:
                            await admin_ws.send(message)
                        except websockets.exceptions.ConnectionClosed:
                            admins.remove(admin_ws)
                else:
                    logging.info(f"Received text from agent: {message}")
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            agent = None
            logging.info("Agent disconnected.")
            # Notify admins that agent went offline
            for admin_ws in list(admins):
                try:
                    await admin_ws.send(json.dumps({"status": "agent_disconnected"}))
                except websockets.exceptions.ConnectionClosed:
                    admins.remove(admin_ws)

async def main():
    # Bind to 0.0.0.0 so it can be exposed to the internet (e.g. via Render or Cloudflare Tunnel)
    host = "0.0.0.0"
    port = int(os.environ.get("PORT", 8765))
    
    # Configure ping_interval and ping_timeout to keep connections alive
    async with websockets.serve(handler, host, port, ping_interval=20, ping_timeout=20):
        logging.info(f"SilentWatch WebSocket Broker running on ws://{host}:{port}")
        await asyncio.Future()  # Run forever

if __name__ == "__main__":
    asyncio.run(main())
