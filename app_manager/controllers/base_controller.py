from app_manager.controllers.app_controller import controller as app_controller


def load_all_controllers(app):
    app.include_router(app_controller)
