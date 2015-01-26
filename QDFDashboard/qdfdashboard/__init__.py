from pyramid.config import Configurator


def main(global_config, **settings):
    """ This function returns a Pyramid WSGI application.
    """
    config = Configurator(settings=settings)
    config.include('pyramid_mako')
    config.add_static_view('static', 'static', cache_max_age=3600)
    #config.add_route('latest', '/')
    config.add_route('logfile', '/log/{id:[0-9a-z\._]+}')
    config.add_route('archive', '/')
    config.add_route('daylist', '/list/{inst}/{year}/{month}/{day}')
    config.scan()
    return config.make_wsgi_app()
