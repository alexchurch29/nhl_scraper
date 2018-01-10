teams = {'ANAHEIM DUCKS': 'ANA', 'ARIZONA COYOTES': 'ARI', 'ATLANTA THRASHERS': 'ATL', 'BOSTON BRUINS': 'BOS',
         'BUFFALO SABRES': 'BUF', 'CAROLINA HURRICANES': 'CAR', 'COLUMBUS BLUE JACKETS': 'CBJ', 'CALGARY FLAMES': 'CGY',
         'CHICAGO BLACKHAWKS': 'CHI', 'COLORADO AVALANCHE': 'COL', 'DALLAS STARS': 'DAL', 'DETROIT RED WINGS': 'DET',
         'EDMONTON OILERS': 'EDM', 'FLORIDA PANTHERS': 'FLA', 'LOS ANGELES KINGS': 'L.A', 'MINNESOTA WILD': 'MIN',
         'MONTREAL CANADIENS': 'MTL', 'MONTRÉAL CANADIENS': 'MTL', 'NEW JERSEY DEVILS': 'N.J',
         'NASHVILLE PREDATORS': 'NSH', 'NEW YORK ISLANDERS': 'NYI', 'NEW YORK RANGERS': 'NYR', 'OTTAWA SENATORS': 'OTT',
         'PHILADELPHIA FLYERS': 'PHI', 'PHOENIX COYOTES': 'PHX', 'PITTSBURGH PENGUINS': 'PIT', 'SAN JOSE SHARKS': 'S.J',
         'ST. LOUIS BLUES': 'STL', 'TAMPA BAY LIGHTNING': 'T.B', 'TORONTO MAPLE LEAFS': 'TOR',
         'VANCOUVER CANUCKS': 'VAN', 'VEGAS GOLDEN KNIGHTS': 'VGK', 'WINNIPEG JETS': 'WPG', 'WASHINGTON CAPITALS': 'WSH'
         }

def fix_team(team):
    """
    Convert full team name to acronym
    :param team: full team name
    :return: team acronym
    """
    if team in teams.keys():
        return teams[team]
    else:
        return team
