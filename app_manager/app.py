from fastapi import FastAPI

from .controllers.base_controller import load_all_controllers

app = FastAPI(title="Catalog Integration")

load_all_controllers(app)





