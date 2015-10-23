import socket

from flask import Flask
from flask.ext.admin import Admin, base

from airflow import login
from airflow import models
from airflow.settings import Session

from airflow.www.blueprints import ck, routes
from airflow import jobs
from airflow import settings
from airflow.configuration import conf


def create_app(config=None):
    app = Flask(__name__)
    app.secret_key = conf.get('webserver', 'SECRET_KEY')
    #app.config = config
    login.login_manager.init_app(app)

    app.register_blueprint(ck, url_prefix='/ck')
    app.register_blueprint(routes)
    app.jinja_env.add_extension("chartkick.ext.charts")

    with app.app_context():
        from airflow.www.views import HomeView
        admin = Admin(
            app, name='Airflow',
            static_url_path='/admin',
            index_view=HomeView(endpoint='', url='/admin'),
            template_mode='bootstrap3',
        )

        from airflow.www import views
        admin.add_view(views.Airflow(name='DAGs'))
        admin.add_view(views.SlaMissModelView(models.SlaMiss, Session, name="SLA Misses", category="Browse"))
        admin.add_view(views.TaskInstanceModelView(models.TaskInstance, Session, name="Task Instances", category="Browse"))
        admin.add_view(views.LogModelView(models.Log, Session, name="Logs", category="Browse"))
        admin.add_view(views.JobModelView(jobs.BaseJob, Session, name="Jobs", category="Browse"))
        admin.add_view(views.QueryView(name='Ad Hoc Query', category="Data Profiling"))
        admin.add_view(views.ChartModelView(models.Chart, Session, name="Charts", category="Data Profiling"))
        admin.add_view(views.KnowEventView(models.KnownEvent, Session, name="Known Events", category="Data Profiling"))
        admin.add_view(views.PoolModelView(models.Pool, Session, name="Pools", category="Admin"))
        admin.add_view(views.ConfigurationView(name='Configuration', category="Admin"))
        admin.add_view(views.UserModelView(models.User, Session, name="Users", category="Admin"))
        admin.add_view(views.ConnectionModelView(models.Connection, Session, name="Connections", category="Admin"))
        admin.add_view(views.VariableView(models.Variable, Session, name="Variables", category="Admin"))
        admin.add_link(base.MenuLink(category='Docs', name='Documentation', url='http://pythonhosted.org/airflow/'))
        admin.add_link(base.MenuLink(category='Docs',name='Github',url='https://github.com/airbnb/airflow'))

        admin.add_view(views.DagModelView(models.DagModel, Session, name=None))
        # Hack to not add this view to the menu
        admin._menu = admin._menu[:-1]

        def integrate_plugins():
            """Integrate plugins to the context"""
            from airflow.plugins_manager import (
                admin_views, flask_blueprints, menu_links)
            for v in admin_views:
                admin.add_view(v)
            for bp in flask_blueprints:
                print(bp)
                app.register_blueprint(bp)
            for ml in menu_links:
                admin.add_link(ml)

        integrate_plugins()

        @app.context_processor
        def jinja_globals():
            return {
<<<<<<< d9e10e9ae94ea63cafa4a0847a8694ce610fad73
                'hostname': socket.gethostname(),
=======
                'name': task.task_id,
                'instances': [
                        task_instances.get((task.task_id, d)) or {
                            'execution_date': d.isoformat(),
                            'task_id': task.task_id
                        }
                    for d in dates],
                children_key: children,
                'num_dep': len(task.upstream_list),
                'operator': task.task_type,
                'retries': task.retries,
                'owner': task.owner,
                'start_date': task.start_date,
                'end_date': task.end_date,
                'depends_on_past': task.depends_on_past,
                'ui_color': task.ui_color,
            }
        data = {
            'name': '[DAG]',
            'children': [recurse_nodes(t, set()) for t in dag.roots],
            'instances': [
                dag_runs.get(d) or {'execution_date': d.isoformat()}
                for d in dates],
        }

        data = json.dumps(data, indent=4, default=utils.json_ser)
        session.commit()
        session.close()

        form = TreeForm(data={'base_date': max_date, 'num_runs': num_runs})
        return self.render(
            'airflow/tree.html',
            operators=sorted(
                list(set([op.__class__ for op in dag.tasks])),
                key=lambda x: x.__name__
            ),
            root=root,
            form=form,
            dag=dag, data=data, blur=blur)

    @expose('/graph')
    @login_required
    @wwwutils.gzipped
    def graph(self):
        session = settings.Session()
        dag_id = request.args.get('dag_id')
        blur = conf.getboolean('webserver', 'demo_mode')
        arrange = request.args.get('arrange', "LR")
        dag = dagbag.get_dag(dag_id)
        if dag_id not in dagbag.dags:
            flash('DAG "{0}" seems to be missing.'.format(dag_id), "error")
            return redirect('/admin/')

        root = request.args.get('root')
        if root:
            dag = dag.sub_dag(
                task_regex=root,
                include_upstream=True,
                include_downstream=False)

        nodes = []
        edges = []
        for task in dag.tasks:
            nodes.append({
                'id': task.task_id,
                'value': {
                    'label': task.task_id,
                    'labelStyle': "fill:{0};".format(task.ui_fgcolor),
                    'style': "fill:{0};".format(task.ui_color),
                }
            })

        def get_upstream(task):
            for t in task.upstream_list:
                edge = {
                    'u': t.task_id,
                    'v': task.task_id,
                }
                if edge not in edges:
                    edges.append(edge)
                    get_upstream(t)

        for t in dag.roots:
            get_upstream(t)

        dttm = request.args.get('execution_date')
        if dttm:
            dttm = dateutil.parser.parse(dttm)
        else:
            dttm = dag.latest_execution_date or datetime.now().date()

        form = GraphForm(data={'execution_date': dttm, 'arrange': arrange})

        task_instances = {
            ti.task_id: utils.alchemy_to_dict(ti)
            for ti in dag.get_task_instances(session, dttm, dttm)
        }
        tasks = {
            t.task_id: {
                'dag_id': t.dag_id,
                'task_type': t.task_type,
>>>>>>> Got the UI to show externaly triggered runs, root object for DAG Run
            }

        @app.teardown_appcontext
        def shutdown_session(exception=None):
            settings.Session.remove()

        return app
