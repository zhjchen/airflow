import socket
import sys
import traceback

from flask._compat import PY2
from flask import (
    Flask, url_for, Markup, Blueprint, redirect,
    flash, Response, render_template)
from flask.ext.admin import Admin, BaseView, expose, AdminIndexView
from flask.ext.admin.form import DateTimePickerWidget
from flask.ext.admin import base
from flask.ext.admin.actions import action
from flask.ext.admin.contrib.sqla import ModelView
from flask.ext.cache import Cache
from flask import request
import sqlalchemy as sqla
from wtforms import (
    widgets,
    Form, DateTimeField, SelectField, TextAreaField, PasswordField, StringField)

from flask import Flask
from flask.ext.admin import Admin, base

from airflow import login
from airflow import models
from airflow.settings import Session
from airflow.utils import AirflowException
from airflow.www import utils as wwwutils


login_required = login.login_required
current_user = login.current_user
logout_user = login.logout_user


from airflow import default_login as login
if conf.getboolean('webserver', 'AUTHENTICATE'):
    try:
        # Environment specific login
        import airflow_login as login
    except ImportError as e:
        logging.error(
            "authenticate is set to True in airflow.cfg, "
            "but airflow_login failed to import %s" % e)
login_required = login.login_required
current_user = login.current_user
logout_user = login.logout_user

AUTHENTICATE = conf.getboolean('webserver', 'AUTHENTICATE')
if AUTHENTICATE is False:
    login_required = lambda x: x

FILTER_BY_OWNER = False
if conf.getboolean('webserver', 'FILTER_BY_OWNER'):
    # filter_by_owner if authentication is enabled and filter_by_owner is true
    FILTER_BY_OWNER = AUTHENTICATE

class VisiblePasswordInput(widgets.PasswordInput):
    def __init__(self, hide_value=False):
        self.hide_value = hide_value


class VisiblePasswordField(PasswordField):
    widget = VisiblePasswordInput()


def superuser_required(f):
    '''
    Decorator for views requiring superuser access
    '''
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if (
            not AUTHENTICATE or
            (not current_user.is_anonymous() and current_user.is_superuser())
        ):
            return f(*args, **kwargs)
        else:
            flash("This page requires superuser privileges", "error")
            return redirect(url_for('admin.index'))
    return decorated_function


def data_profiling_required(f):
    '''
    Decorator for views requiring data profiling access
    '''
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if (
            not AUTHENTICATE or
            (not current_user.is_anonymous() and current_user.data_profiling())
        ):
            return f(*args, **kwargs)
        else:
            flash("This page requires data profiling privileges", "error")
            return redirect(url_for('admin.index'))
    return decorated_function

QUERY_LIMIT = 100000
CHART_LIMIT = 200000


def pygment_html_render(s, lexer=lexers.TextLexer):
    return highlight(
        s,
        lexer(),
        HtmlFormatter(linenos=True),
    )


def wrapped_markdown(s):
    return '<div class="rich_doc">' + markdown.markdown(s) + "</div>"

def render(obj, lexer):
    out = ""
    if isinstance(obj, basestring):
        out += pygment_html_render(obj, lexer)
    elif isinstance(obj, (tuple, list)):
        for i, s in enumerate(obj):
            out += "<div>List item #{}</div>".format(i)
            out += "<div>" + pygment_html_render(s, lexer) + "</div>"
    elif isinstance(obj, dict):
        for k, v in obj.items():
            out += '<div>Dict item "{}"</div>'.format(k)
            out += "<div>" + pygment_html_render(v, lexer) + "</div>"
    return out


attr_renderer = {
    'bash_command': lambda x: render(x, lexers.BashLexer),
    'hql': lambda x: render(x, lexers.SqlLexer),
    'sql': lambda x: render(x, lexers.SqlLexer),
    'doc': lambda x: render(x, lexers.TextLexer),
    'doc_json': lambda x: render(x, lexers.JsonLexer),
    'doc_rst': lambda x: render(x, lexers.RstLexer),
    'doc_yaml': lambda x: render(x, lexers.YamlLexer),
    'doc_md': wrapped_markdown,
    'python_callable': lambda x: render(
        inspect.getsource(x), lexers.PythonLexer),
}


dagbag = models.DagBag(os.path.expanduser(conf.get('core', 'DAGS_FOLDER')))
utils.pessimistic_connection_handling()

app = Flask(__name__)
app.config['SQLALCHEMY_POOL_RECYCLE'] = 3600
app.secret_key = conf.get('webserver', 'SECRET_KEY')

login.login_manager.init_app(app)

cache = Cache(
    app=app, config={'CACHE_TYPE': 'filesystem', 'CACHE_DIR': '/tmp'})

# Init for chartkick, the python wrapper for highcharts
ck = Blueprint(
    'ck_page', __name__,
    static_folder=chartkick.js(), static_url_path='/static')
app.register_blueprint(ck, url_prefix='/ck')
app.jinja_env.add_extension("chartkick.ext.charts")


@app.context_processor
def jinja_globals():
    return {
        'hostname': socket.gethostname(),
    }


class DateTimeForm(Form):
    # Date filter form needed for gantt and graph view
    execution_date = DateTimeField(
        "Execution date", widget=DateTimePickerWidget())


class TreeForm(Form):
    base_date = DateTimeField(
        "Anchor date", widget=DateTimePickerWidget(), default=datetime.now())
    num_runs = SelectField("Number of runs", default=25, choices=(
        (5, "5"),
        (25, "25"),
        (50, "50"),
        (100, "100"),
        (365, "365"),
    ))


@app.route('/')
def index():
    return redirect(url_for('admin.index'))


@app.route('/health')
def health():
    """ We can add an array of tests here to check the server's health """
    content = Markup(markdown.markdown("The server is healthy!"))
    return content


@app.teardown_appcontext
def shutdown_session(exception=None):
    settings.Session.remove()


def dag_link(v, c, m, p):
    url = url_for(
        'airflow.graph',
        dag_id=m.dag_id)
    return Markup(
        '<a href="{url}">{m.dag_id}</a>'.format(**locals()))


class DagModelView(wwwutils.SuperUserMixin, ModelView):
    column_list = ('dag_id', 'owners')
    column_editable_list = ('is_paused',)
    form_excluded_columns = ('is_subdag', 'is_active')
    column_searchable_list = ('dag_id',)
    column_filters = (
        'dag_id', 'owners', 'is_paused', 'is_active', 'is_subdag',
        'last_scheduler_run', 'last_expired')
    form_widget_args = {
        'last_scheduler_run': {'disabled': True},
        'fileloc': {'disabled': True},
        'is_paused': {'disabled': True},
        'last_pickled': {'disabled': True},
        'pickle_id': {'disabled': True},
        'last_loaded': {'disabled': True},
        'last_expired': {'disabled': True},
        'pickle_size': {'disabled': True},
        'scheduler_lock': {'disabled': True},
        'owners': {'disabled': True},
    }
    column_formatters = dict(
        dag_id=dag_link,
    )
    can_delete = False
    can_create = False
    page_size = 50
    list_template = 'airflow/list_dags.html'
    named_filter_urls = True

    def get_query(self):
        """
        Default filters for model
        """
        return (
            super(DagModelView, self)
            .get_query()
            .filter(or_(models.DagModel.is_active, models.DagModel.is_paused))
            .filter(~models.DagModel.is_subdag)
        )

    def get_count_query(self):
        """
        Default filters for model
        """
        return (
            super(DagModelView, self)
            .get_count_query()
            .filter(models.DagModel.is_active)
            .filter(~models.DagModel.is_subdag)
        )


class HomeView(AdminIndexView):
    @expose("/")
    @login_required
    def index(self):
        session = Session()
        DM = models.DagModel
        qry = None
        # filter the dags if filter_by_owner and current user is not superuser
        do_filter = FILTER_BY_OWNER and (not current_user.is_superuser())
        if do_filter:
            qry = (
                session.query(DM)
                .filter(
                    ~DM.is_subdag, DM.is_active,
                    DM.owners == current_user.username)
                .all()
            )
        else:
            qry = session.query(DM).filter(~DM.is_subdag, DM.is_active).all()
        orm_dags = {dag.dag_id: dag for dag in qry}
        import_errors = session.query(models.ImportError).all()
        for ie in import_errors:
            flash(
                "Broken DAG: [{ie.filename}] {ie.stacktrace}".format(ie=ie),
                "error")
        session.expunge_all()
        session.commit()
        session.close()
        dags = dagbag.dags.values()
        if do_filter:
            dags = {
                dag.dag_id: dag
                for dag in dags
                if (
                    dag.owner == current_user.username and (not dag.parent_dag)
                )
            }
        else:
            dags = {dag.dag_id: dag for dag in dags if not dag.parent_dag}
        all_dag_ids = sorted(set(orm_dags.keys()) | set(dags.keys()))
        return self.render(
            'airflow/dags.html',
            dags=dags,
            orm_dags=orm_dags,
            all_dag_ids=all_dag_ids)

admin = Admin(
    app,
    name="Airflow",
    index_view=HomeView(name="DAGs"),
    template_mode='bootstrap3')


class Airflow(BaseView):

    def is_visible(self):
        return False

    @expose('/')
    @login_required
    def index(self):
        return self.render('airflow/dags.html')

    @expose('/chart_data')
    @data_profiling_required
    @wwwutils.gzipped
    # @cache.cached(timeout=3600, key_prefix=wwwutils.make_cache_key)
    def chart_data(self):
        session = settings.Session()
        chart_id = request.args.get('chart_id')
        csv = request.args.get('csv') == "true"
        chart = session.query(models.Chart).filter_by(id=chart_id).first()
        db = session.query(
            models.Connection).filter_by(conn_id=chart.conn_id).first()
        session.expunge_all()
        session.commit()
        session.close()

        payload = {}
        payload['state'] = 'ERROR'
        payload['error'] = ''

        # Processing templated fields
        try:
            args = eval(chart.default_params)
            if type(args) is not type(dict()):
                raise AirflowException('Not a dict')
        except:
            args = {}
            payload['error'] += (
                "Default params is not valid, string has to evaluate as "
                "a Python dictionary. ")

        request_dict = {k: request.args.get(k) for k in request.args}
        from airflow import macros
        args.update(request_dict)
        args['macros'] = macros
        sql = jinja2.Template(chart.sql).render(**args)
        label = jinja2.Template(chart.label).render(**args)
        payload['sql_html'] = Markup(highlight(
            sql,
            lexers.SqlLexer(),  # Lexer call
            HtmlFormatter(noclasses=True))
        )
        payload['label'] = label

        import pandas as pd
        pd.set_option('display.max_colwidth', 100)
        hook = db.get_hook()
        try:
            df = hook.get_pandas_df(wwwutils.limit_sql(sql, CHART_LIMIT, conn_type=db.conn_type))
            df = df.fillna(0)
        except Exception as e:
            payload['error'] += "SQL execution failed. Details: " + str(e)

        if csv:
            return Response(
                response=df.to_csv(index=False),
                status=200,
                mimetype="application/text")

        if not payload['error'] and len(df) == CHART_LIMIT:
            payload['warning'] = (
                "Data has been truncated to {0}"
                " rows. Expect incomplete results.").format(CHART_LIMIT)

        if not payload['error'] and len(df) == 0:
            payload['error'] += "Empty result set. "
        elif (
                not payload['error'] and
                chart.sql_layout == 'series' and
                chart.chart_type != "datatable" and
                len(df.columns) < 3):
            payload['error'] += "SQL needs to return at least 3 columns. "
        elif (
                not payload['error'] and
                chart.sql_layout == 'columns'and
                len(df.columns) < 2):
            payload['error'] += "SQL needs to return at least 2 columns. "
        elif not payload['error']:
            import numpy as np
            chart_type = chart.chart_type

            data = None
            if chart_type == "datatable":
                chart.show_datatable = True
            if chart.show_datatable:
                data = df.to_dict(orient="split")
                data['columns'] = [{'title': c} for c in data['columns']]

            # Trying to convert time to something Highcharts likes
            x_col = 1 if chart.sql_layout == 'series' else 0
            if chart.x_is_date:
                try:
                    # From string to datetime
                    df[df.columns[x_col]] = pd.to_datetime(
                        df[df.columns[x_col]])
                except Exception as e:
                    raise AirflowException(str(e))
                df[df.columns[x_col]] = df[df.columns[x_col]].apply(
                    lambda x: int(x.strftime("%s")) * 1000)

            series = []
            colorAxis = None
            if chart_type == 'datatable':
                payload['data'] = data
                payload['state'] = 'SUCCESS'
                return Response(
                    response=json.dumps(
                        payload, indent=4, cls=utils.AirflowJsonEncoder),
                    status=200,
                    mimetype="application/json")

            elif chart_type == 'para':
                df.rename(columns={
                    df.columns[0]: 'name',
                    df.columns[1]: 'group',
                }, inplace=True)
                return Response(
                    response=df.to_csv(index=False),
                    status=200,
                    mimetype="application/text")

            elif chart_type == 'heatmap':
                color_perc_lbound = float(
                    request.args.get('color_perc_lbound', 0))
                color_perc_rbound = float(
                    request.args.get('color_perc_rbound', 1))
                color_scheme = request.args.get('color_scheme', 'blue_red')

                if color_scheme == 'blue_red':
                    stops = [
                        [color_perc_lbound, '#00D1C1'],
                        [
                            color_perc_lbound +
                            ((color_perc_rbound - color_perc_lbound)/2),
                            '#FFFFCC'
                        ],
                        [color_perc_rbound, '#FF5A5F']
                    ]
                elif color_scheme == 'blue_scale':
                    stops = [
                        [color_perc_lbound, '#FFFFFF'],
                        [color_perc_rbound, '#2222FF']
                    ]
                elif color_scheme == 'fire':
                    diff = float(color_perc_rbound - color_perc_lbound)
                    stops = [
                        [color_perc_lbound, '#FFFFFF'],
                        [color_perc_lbound + 0.33*diff, '#FFFF00'],
                        [color_perc_lbound + 0.66*diff, '#FF0000'],
                        [color_perc_rbound, '#000000']
                    ]
                else:
                    stops = [
                        [color_perc_lbound, '#FFFFFF'],
                        [
                            color_perc_lbound +
                            ((color_perc_rbound - color_perc_lbound)/2),
                            '#888888'
                        ],
                        [color_perc_rbound, '#000000'],
                    ]

                xaxis_label = df.columns[1]
                yaxis_label = df.columns[2]
                data = []
                for row in df.itertuples():
                    data.append({
                        'x': row[2],
                        'y': row[3],
                        'value': row[4],
                    })
                x_format = '{point.x:%Y-%m-%d}' \
                    if chart.x_is_date else '{point.x}'
                series.append({
                    'data': data,
                    'borderWidth': 0,
                    'colsize': 24 * 36e5,
                    'turboThreshold': sys.float_info.max,
                    'tooltip': {
                        'headerFormat': '',
                        'pointFormat': (
                            df.columns[1] + ': ' + x_format + '<br/>' +
                            df.columns[2] + ': {point.y}<br/>' +
                            df.columns[3] + ': <b>{point.value}</b>'
                        ),
                    },
                })
                colorAxis = {
                    'stops': stops,
                    'minColor': '#FFFFFF',
                    'maxColor': '#000000',
                    'min': 50,
                    'max': 2200,
                }
            else:
                if chart.sql_layout == 'series':
                    # User provides columns (series, x, y)
                    xaxis_label = df.columns[1]
                    yaxis_label = df.columns[2]
                    df[df.columns[2]] = df[df.columns[2]].astype(np.float)
                    df = df.pivot_table(
                        index=df.columns[1],
                        columns=df.columns[0],
                        values=df.columns[2], aggfunc=np.sum)
                else:
                    # User provides columns (x, y, metric1, metric2, ...)
                    xaxis_label = df.columns[0]
                    yaxis_label = 'y'
                    df.index = df[df.columns[0]]
                    df = df.sort(df.columns[0])
                    del df[df.columns[0]]
                    for col in df.columns:
                        df[col] = df[col].astype(np.float)

                for col in df.columns:
                    series.append({
                        'name': col,
                        'data': [
                            (k, df[col][k])
                            for k in df[col].keys()
                            if not np.isnan(df[col][k])]
                    })
                series = [serie for serie in sorted(
                    series, key=lambda s: s['data'][0][1], reverse=True)]

            if chart_type == "stacked_area":
                stacking = "normal"
                chart_type = 'area'
            elif chart_type == "percent_area":
                stacking = "percent"
                chart_type = 'area'
            else:
                stacking = None
            hc = {
                'chart': {
                    'type': chart_type
                },
                'plotOptions': {
                    'series': {
                        'marker': {
                            'enabled': False
                        }
                    },
                    'area': {'stacking': stacking},
                },
                'title': {'text': ''},
                'xAxis': {
                    'title': {'text': xaxis_label},
                    'type': 'datetime' if chart.x_is_date else None,
                },
                'yAxis': {
                    'title': {'text': yaxis_label},
                },
                'colorAxis': colorAxis,
                'tooltip': {
                    'useHTML': True,
                    'backgroundColor': None,
                    'borderWidth': 0,
                },
                'series': series,
            }

            if chart.y_log_scale:
                hc['yAxis']['type'] = 'logarithmic'
                hc['yAxis']['minorTickInterval'] = 0.1
                if 'min' in hc['yAxis']:
                    del hc['yAxis']['min']

            payload['state'] = 'SUCCESS'
            payload['hc'] = hc
            payload['data'] = data
            payload['request_dict'] = request_dict

        return Response(
            response=json.dumps(
                payload, indent=4, cls=utils.AirflowJsonEncoder),
            status=200,
            mimetype="application/json")

    @expose('/chart')
    @data_profiling_required
    def chart(self):
        session = settings.Session()
        chart_id = request.args.get('chart_id')
        embed = request.args.get('embed')
        chart = session.query(models.Chart).filter_by(id=chart_id).first()
        session.expunge_all()
        session.commit()
        session.close()
        if chart.chart_type == 'para':
            return self.render('airflow/para/para.html', chart=chart)

        sql = ""
        if chart.show_sql:
            sql = Markup(highlight(
                chart.sql,
                lexers.SqlLexer(),  # Lexer call
                HtmlFormatter(noclasses=True))
            )
        return self.render(
            'airflow/highchart.html',
            chart=chart,
            title="Airflow - Chart",
            sql=sql,
            label=chart.label,
            embed=embed)

    @expose('/dag_stats')
    @login_required
    def dag_stats(self):
        states = [
            State.SUCCESS,
            State.RUNNING,
            State.FAILED,
            State.UPSTREAM_FAILED,
            State.UP_FOR_RETRY,
            State.QUEUED,
        ]
        task_ids = []
        dag_ids = []
        for dag in dagbag.dags.values():
            task_ids += dag.task_ids
            if not dag.is_subdag:
                dag_ids.append(dag.dag_id)
        TI = models.TaskInstance
        session = Session()
        qry = (
            session.query(TI.dag_id, TI.state, sqla.func.count(TI.task_id))
            .filter(TI.task_id.in_(task_ids))
            .filter(TI.dag_id.in_(dag_ids))
            .group_by(TI.dag_id, TI.state)
        )

        data = {}
        for dag_id, state, count in qry:
            if dag_id not in data:
                data[dag_id] = {}
            data[dag_id][state] = count
        session.commit()
        session.close()

        payload = {}
        for dag in dagbag.dags.values():
            payload[dag.safe_dag_id] = []
            for state in states:
                try:
                    count = data[dag.dag_id][state]
                except:
                    count = 0
                d = {
                    'state': state,
                    'count': count,
                    'dag_id': dag.dag_id,
                    'color': State.color(state)
                }
                payload[dag.safe_dag_id].append(d)
        return Response(
            response=json.dumps(payload, indent=4),
            status=200, mimetype="application/json")

    @expose('/code')
    @login_required
    def code(self):
        dag_id = request.args.get('dag_id')
        dag = dagbag.get_dag(dag_id)
        code = "".join(open(dag.full_filepath, 'r').readlines())
        title = dag.filepath
        html_code = highlight(
            code, lexers.PythonLexer(), HtmlFormatter(linenos=True))
        return self.render(
            'airflow/dag_code.html', html_code=html_code, dag=dag, title=title,
            root=request.args.get('root'),
            demo_mode=conf.getboolean('webserver', 'demo_mode'))

    @app.errorhandler(404)
    def circles(self):
        return render_template(
            'airflow/circles.html', hostname=socket.gethostname()), 404

    @app.errorhandler(500)
    def show_traceback(self):
        return render_template(
            'airflow/traceback.html', info=traceback.format_exc()), 500

    @expose('/sandbox')
    @login_required
    def sandbox(self):
        from airflow import configuration
        title = "Sandbox Suggested Configuration"
        cfg_loc = configuration.AIRFLOW_CONFIG + '.sandbox'
        f = open(cfg_loc, 'r')
        config = f.read()
        f.close()
        code_html = Markup(highlight(
            config,
            lexers.IniLexer(),  # Lexer call
            HtmlFormatter(noclasses=True))
        )
        return self.render(
            'airflow/code.html',
            code_html=code_html, title=title, subtitle=cfg_loc)

    @expose('/noaccess')
    def noaccess(self):
        return self.render('airflow/noaccess.html')

    @expose('/headers')
    def headers(self):
        d = {k: v for k, v in request.headers}
        if hasattr(current_user, 'is_superuser'):
            d['is_superuser'] = current_user.is_superuser()
            d['data_profiling'] = current_user.data_profiling()
            d['is_anonymous'] = current_user.is_anonymous()
            d['is_authenticated'] = current_user.is_authenticated()
        return Response(
            response=json.dumps(d, indent=4),
            status=200, mimetype="application/json")

    @expose('/login', methods=['GET', 'POST'])
    def login(self):
        return login.login(self, request)

    @expose('/logout')
    def logout(self):
        logout_user()
        return redirect(url_for('admin.index'))

    @expose('/rendered')
    @login_required
    def rendered(self):
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        execution_date = request.args.get('execution_date')
        dttm = dateutil.parser.parse(execution_date)
        form = DateTimeForm(data={'execution_date': dttm})
        dag = dagbag.get_dag(dag_id)
        task = copy.copy(dag.get_task(task_id))
        ti = models.TaskInstance(task=task, execution_date=dttm)
        try:
            ti.render_templates()
        except Exception as e:
            flash("Error rendering template: " + str(e), "error")
        title = "Rendered Template"
        html_dict = {}
        for template_field in task.__class__.template_fields:
            content = getattr(task, template_field)
            if template_field in attr_renderer:
                html_dict[template_field] = attr_renderer[template_field](content)
            else:
                html_dict[template_field] = (
                    "<pre><code>" + str(content) + "</pre></code>")

        return self.render(
            'airflow/ti_code.html',
            html_dict=html_dict,
            dag=dag,
            task_id=task_id,
            execution_date=execution_date,
            form=form,
            title=title,)

    @expose('/log')
    @login_required
    def log(self):
        BASE_LOG_FOLDER = os.path.expanduser(
            conf.get('core', 'BASE_LOG_FOLDER'))
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        execution_date = request.args.get('execution_date')
        dag = dagbag.get_dag(dag_id)
        log_relative = "{dag_id}/{task_id}/{execution_date}".format(
            **locals())
        loc = os.path.join(BASE_LOG_FOLDER, log_relative)
        loc = loc.format(**locals())
        log = ""
        TI = models.TaskInstance
        session = Session()
        dttm = dateutil.parser.parse(execution_date)
        ti = session.query(TI).filter(
            TI.dag_id == dag_id, TI.task_id == task_id,
            TI.execution_date == dttm).first()
        dttm = dateutil.parser.parse(execution_date)
        form = DateTimeForm(data={'execution_date': dttm})

        if ti:
            host = ti.hostname
            log_loaded = False

            if socket.gethostname() == host:
                try:
                    f = open(loc)
                    log += "".join(f.readlines())
                    f.close()
                    log_loaded = True
                except:
                    log = "*** Log file isn't where expected.\n".format(loc)
            else:
                WORKER_LOG_SERVER_PORT = \
                    conf.get('celery', 'WORKER_LOG_SERVER_PORT')
                url = os.path.join(
                    "http://{host}:{WORKER_LOG_SERVER_PORT}/log", log_relative
                    ).format(**locals())
                log += "*** Log file isn't local.\n"
                log += "*** Fetching here: {url}\n".format(**locals())
                try:
                    import requests
                    log += '\n' + requests.get(url).text
                    log_loaded = True
                except:
                    log += "*** Failed to fetch log file from worker.\n".format(
                        **locals())

            # try to load log backup from S3
            s3_log_folder = conf.get('core', 'S3_LOG_FOLDER')
            if not log_loaded and s3_log_folder.startswith('s3:'):
                import boto
                s3 = boto.connect_s3()
                s3_log_loc = os.path.join(
                    conf.get('core', 'S3_LOG_FOLDER'), log_relative)
                log += '*** Fetching log from S3: {}\n'.format(s3_log_loc)
                log += ('*** Note: S3 logs are only available once '
                        'tasks have completed.\n')
                bucket, key = s3_log_loc.lstrip('s3:/').split('/', 1)
                s3_key = boto.s3.key.Key(s3.get_bucket(bucket), key)
                if s3_key.exists():
                    log += '\n' + s3_key.get_contents_as_string().decode()
                else:
                    log += '*** No log found on S3.\n'

            session.commit()
            session.close()
        log = log.decode('utf-8') if PY2 else log

        title = "Log"

        return self.render(
            'airflow/ti_code.html',
            code=log, dag=dag, title=title, task_id=task_id,
            execution_date=execution_date, form=form)

    @expose('/task')
    @login_required
    def task(self):
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        # Carrying execution_date through, even though it's irrelevant for
        # this context
        execution_date = request.args.get('execution_date')
        dttm = dateutil.parser.parse(execution_date)
        form = DateTimeForm(data={'execution_date': dttm})
        dag = dagbag.get_dag(dag_id)
        if not dag or task_id not in dag.task_ids:
            flash(
                "Task [{}.{}] doesn't seem to exist"
                " at the moment".format(dag_id, task_id),
                "error")
            return redirect('/admin/')
        task = dag.get_task(task_id)
        task = copy.copy(task)
        task.resolve_template_files()

        attributes = []
        for attr_name in dir(task):
            if not attr_name.startswith('_'):
                attr = getattr(task, attr_name)
                if type(attr) != type(self.task) and \
                        attr_name not in attr_renderer:
                    attributes.append((attr_name, str(attr)))

        title = "Task Details"
        # Color coding the special attributes that are code
        special_attrs_rendered = {}
        for attr_name in attr_renderer:
            if hasattr(task, attr_name):
                source = getattr(task, attr_name)
                special_attrs_rendered[attr_name] = attr_renderer[attr_name](source)

        return self.render(
            'airflow/task.html',
            attributes=attributes,
            task_id=task_id,
            execution_date=execution_date,
            special_attrs_rendered=special_attrs_rendered,
            form=form,
            dag=dag, title=title)

    @expose('/action')
    @login_required
    def action(self):
        action = request.args.get('action')
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        origin = request.args.get('origin')
        dag = dagbag.get_dag(dag_id)
        task = dag.get_task(task_id)

        execution_date = request.args.get('execution_date')
        execution_date = dateutil.parser.parse(execution_date)
        confirmed = request.args.get('confirmed') == "true"
        upstream = request.args.get('upstream') == "true"
        downstream = request.args.get('downstream') == "true"
        future = request.args.get('future') == "true"
        past = request.args.get('past') == "true"

        if action == "run":
            from airflow.executors import DEFAULT_EXECUTOR as executor
            from airflow.executors import CeleryExecutor
            if not isinstance(executor, CeleryExecutor):
                flash("Only works with the CeleryExecutor, sorry", "error")
                return redirect(origin)
            force = request.args.get('force') == "true"
            deps = request.args.get('deps') == "true"
            ti = models.TaskInstance(task=task, execution_date=execution_date)
            executor.start()
            executor.queue_task_instance(
                ti, force=force, ignore_dependencies=deps)
            executor.heartbeat()
            flash(
                "Sent {} to the message queue, "
                "it should start any moment now.".format(ti))
            return redirect(origin)

        elif action == 'clear':
            dag = dag.sub_dag(
                task_regex=r"^{0}$".format(task_id),
                include_downstream=downstream,
                include_upstream=upstream)

            end_date = execution_date if not future else None
            start_date = execution_date if not past else None
            if confirmed:

                count = dag.clear(
                    start_date=start_date,
                    end_date=end_date)

                flash("{0} task instances have been cleared".format(count))
                return redirect(origin)
            else:
                tis = dag.clear(
                    start_date=start_date,
                    end_date=end_date,
                    dry_run=True)
                if not tis:
                    flash("No task instances to clear", 'error')
                    response = redirect(origin)
                else:
                    details = "\n".join([str(t) for t in tis])

                    response = self.render(
                        'airflow/confirm.html',
                        message=(
                            "Here's the list of task instances you are about "
                            "to clear:"),
                        details=details,)

                return response
        elif action == 'success':
            MAX_PERIODS = 1000

            # Flagging tasks as successful
            session = settings.Session()
            task_ids = [task_id]
            end_date = ((dag.latest_execution_date or datetime.now())
                        if future else execution_date)

            if 'start_date' in dag.default_args:
                start_date = dag.default_args['start_date']
            elif dag.start_date:
                start_date = dag.start_date
            else:
                start_date = execution_date

            if execution_date < start_date or end_date < start_date:
                flash("Selected date before DAG start date", 'error')
                return redirect(origin)

            start_date = execution_date if not past else start_date

            if downstream:
                task_ids += [
                    t.task_id
                    for t in task.get_flat_relatives(upstream=False)]
            if upstream:
                task_ids += [
                    t.task_id
                    for t in task.get_flat_relatives(upstream=True)]
            TI = models.TaskInstance
            dates = utils.date_range(start_date, end_date)
            tis = session.query(TI).filter(
                TI.dag_id == dag_id,
                TI.execution_date.in_(dates),
                TI.task_id.in_(task_ids)).all()
            tis_to_change = session.query(TI).filter(
                TI.dag_id == dag_id,
                TI.execution_date.in_(dates),
                TI.task_id.in_(task_ids),
                TI.state != State.SUCCESS).all()
            tasks = list(product(task_ids, dates))
            tis_to_create = list(
                set(tasks) -
                set([(ti.task_id, ti.execution_date) for ti in tis]))
>>>>>>> Adapting the UI

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
                'hostname': socket.gethostname(),
            }

        @app.teardown_appcontext
        def shutdown_session(exception=None):
            settings.Session.remove()

        return app
