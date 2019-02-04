import datetime

from dataflows import Flow, load, dump_to_path, PackageWrapper, ResourceWrapper,printer

def set_format_and_name(package: PackageWrapper):
    package.pkg.descriptor['title'] = 'London underground performance'
    package.pkg.descriptor['name'] = 'london-underground-performance'
    # Change path and name for the resource:
    package.pkg.descriptor['resources'][0]['path'] = 'data/key-trends.csv'
    package.pkg.descriptor['resources'][0]['name'] = 'key-trends'

    package.pkg.descriptor['resources'][1]['path'] = 'data/lost-customers-hours.csv'
    package.pkg.descriptor['resources'][1]['name'] = 'lost-customers-hours'

    yield package.pkg
    res_iter = iter(package)
    first: ResourceWrapper = next(res_iter)
    second: ResourceWrapper = next(res_iter)
    yield first.it
    yield second.it
    yield from package

def filter_underground_journeys(rows):
    for row in rows:
        if row['Days in period'] is not '':
            yield row

def london_underground_journeys(link):
    Flow(
        load(link,
             sheet=2),
        filter_underground_journeys,
        load(link,
             sheet=3),
        set_format_and_name,
        dump_to_path('data'),
        printer(num_rows=1)
    ).process()


london_underground_journeys('https://data.london.gov.uk/download/london-underground-performance-reports/c3ecab2b-5acf-4124-8dd5-fb4ae7c016e5/tfl-tube-performance.xls')