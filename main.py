import logging
import tornado.web
import json
import tornado.options
import time
from helper.com_key_handler import validate_communication_key
import psycopg
from psycopg.rows import dict_row
import re


# Subclassing tornado class to have custom tornado web app.
# tornado.web.Application is the main app object in Tornado that
# manages routes/handlers, settings, and other global state.
#
# Inherits all the features of Tornado’s Application
# By subclassing it we can add db connection
class App(tornado.web.Application):
    # Constructor.
    # handlers is a list of URL routes and their corresponding RequestHandlers.
    # **kwargs allows passing additional Tornado settings like debug=True or cookie_secret="...".
    # We need this to have extra add-ons when creating the app (db connection)
    def __init__(self, handlers, **kwargs):
        # This calls the constructor of the parent class (tornado.web.Application) with the same parameters.
        # Call the parent to set up Tornado’s app exactly as it normally would, with all the routing,
        # request handling, and default internal setup.
        #
        # Then, subclass (App) can add custom behavior—like connecting to a database, adding extra
        # properties, or initializing services—without breaking Tornado’s normal behavior.
        super().__init__(handlers, **kwargs)

        # PostgreSQL. It is granted for Read and Write
        # User for the database in db tools, will be better to use read only user
        self.db = psycopg.connect(
            host="localhost",
            port=5433,
            dbname="users",
            user="users",
            password="12345"
        )
        self.init_db()

    # Use self to couple the function / attributes to a class to make it is instance-specific
    def init_db(self):
        with self.db.cursor(row_factory=dict_row) as cursor:
            # Create table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    created_at BIGINT NOT NULL,
                    updated_at BIGINT NOT NULL
                );
                """
            )
        self.db.commit()


# Subclassing RequestHandler
# Inherits what ever Request Handler has by using self
class BaseHandler(tornado.web.RequestHandler):
    # We write the response in JSON
    def write_json(self, obj, status_code=200):
        self.set_header("Content-Type", "application/json")
        self.set_status(status_code)
        self.write(json.dumps(obj))


# Subclassing BaseHandler
# Inherits what ever BaseHandler has by using self
class UsersHandler(BaseHandler):
    # Asynchronous 11:12 to async def, but we can use yield to wait for a future
    @tornado.gen.coroutine
    def post(self):
        # We validate the authentication key here to grant access for other service
        error = validate_communication_key(self.request.headers.get("Com-X-Key"))
        if error is not None:
            self.write_json({"result": False, "error": error}, status_code=401)
            return
        name_arg = self.get_argument("name")

        name, error = self._validate_name(name_arg)
        if error is not None:
            logging.exception(error)
            self.write_json({"result": False, "error": error}, status_code=400)
            return
        time_now = int(time.time() * 1e6)

        # self.application from Request Handler has a reference to the Application instance
        # it know add-ons that we made when we subclassed the tornado app
        # cursor is what we use to run SQL queries
        # row_factory=dict_row will modify query result from tuples to dictionary
        cursor = self.application.db.cursor(row_factory=dict_row)
        cursor.execute(
            "INSERT INTO users (name, created_at, updated_at) VALUES (%s, %s, %s) RETURNING id",
            (name, time_now, time_now)
        )
        row = cursor.fetchone()
        self.application.db.commit()

        if not row:
            logging.exception(f"Failed to insert data into database.")
            self.write_json({"result": False, "error": "Failed to insert data into database"}, status_code=500)
            return
        user = {
            "id": row["id"],
            "name": name,
            "created_at": time_now,
            "updated_at": time_now
        }

        self.write_json({"result": True, "user": user})

    @tornado.gen.coroutine
    def get(self):
        error = validate_communication_key(self.request.headers.get("Com-X-Key"))
        if error is not None:
            self.write_json({"result": False, "error": error}, status_code=401)
            return
        page_num = self.get_argument("page_num", 1)
        page_size = self.get_argument("page_size", 10)
        try:
            page_num = int(page_num)
        except:
            logging.exception(f"Error while parsing page_num : {page_num}")
            self.write_json({"result": False, "error": "Invalid page_num"}, status_code=400)
            return

        try:
            page_size = int(page_size)
        except:
            logging.exception(f"Error while parsing page_size : {page_size}")
            self.write_json({"result": False, "error": "Invalid page_size"}, status_code=400)
            return

        limit = page_size
        offset = (page_num - 1) * page_size
        cursor = self.application.db.cursor(row_factory=dict_row)
        results = cursor.execute("SELECT * FROM users ORDER BY created_at DESC LIMIT %s OFFSET %s",
                                 (limit, offset))
        users = []
        for result in results:
            user = {
                "id": result["id"],
                "name": result["name"],
                "created_at": result["created_at"],
                "updated_at": result["updated_at"]
            }
            users.append(user)

        self.write_json({"result": True, "users": users})

    def _validate_name(self, name):
        try:
            name = str(name)
            if name == "":
                return None, "Name cannot be empty"
            if re.search(r'[\d\W]',name.replace(" ", "")):
                return None, "Name cannot be filled with number or symbol"
            return name, None
        except Exception as e:
            logging.exception(f"Error while parsing name : {name}, {e}")
            return None, "Invalid name"


class UserDetailHandler(BaseHandler):
    @tornado.gen.coroutine
    def get(self, user_id):
        error = validate_communication_key(self.request.headers.get("Com-X-Key"))
        if error is not None:
            self.write_json({"result": False, "error": error}, status_code=401)
            return
        user_id_validated, error = self._validate_user_id(user_id)
        if error is not None:
            logging.exception(error)
            self.write_json({"result": False, "error": error}, status_code=400)
            return
        cursor = self.application.db.cursor(row_factory=dict_row)
        result = cursor.execute("SELECT * FROM users WHERE id = %s", (user_id_validated,)).fetchone()
        if not result:
            logging.exception("User is not found")
            self.write_json({"result": False, "error": "User is not found"}, status_code=404)
            return
        user = {
            "id": result["id"],
            "name": result["name"],
            "created_at": result["created_at"],
            "updated_at": result["updated_at"]
        }
        self.write_json({"result": True, "user": user}, status_code=200)

    def _validate_user_id(self, user_id):
        try:
            user_id = int(user_id)
            if user_id == 0:
                return None, "User ID cannot be 0"
            return user_id, None
        except Exception as e:
            logging.exception(f"Error while parsing user_id : {user_id}, {e}")
            return None, "Invalid user_id"

# this one is not subclassing
# just pass an object
def make_app(options):
    # Create instance of App, our subclass from tornado web app
    return App([
        # The routes with its handler
        # r means raw string
        (r"/users", UsersHandler),
        (r"/users/([0-9]+)", UserDetailHandler)
        # optional argument to enable debug when it is true
    ], debug=options.debug)


if __name__ == "__main__":
    # get the port from command line if it is not defined use the default
    tornado.options.define("port", default=6001)
    tornado.options.define("debug", default=True)
    # reads the command line, parses any option in command line, and store it to be accessed
    tornado.options.parse_command_line()
    # get the stored command line
    options = tornado.options.options

    app = make_app(options)
    app.listen(options.port)
    logging.info(f"Service is running at port {options.port}, debug level is {options.debug}")

    # will loop, and with coroutine when there is 2 process, A process is currently handled by the thread,
    # when it is paused by yield / await to wait a future, the thread can leave it to jump to another process
    # that is ready to be served, and when A is ready to be served again, thread will come back :)
    tornado.ioloop.IOLoop.instance().start()
