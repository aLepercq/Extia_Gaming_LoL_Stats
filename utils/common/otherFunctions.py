import pandas as pd


# apply ranking algorithm
def scoring(row):
    if row['position'] == 'TOP':
        return row['damageDealtToObjectives'] * 0.1 + row['damageSelfMitigated'] * 0.1 + \
            row['damageTakenOnTeamPercentage'] * 0.3 + row['damagePerMinute'] * 0.3 + row['kda'] * 0.1 +\
            row['killParticipation'] * 0.1 + row['winrate'] * 0.2
    # add CC score, remove KP
    elif row['position'] == 'JGL':
        return row['killParticipation'] * 0.2 + row['damageDealtToObjectives'] * 0.2 + row['kda'] * 0.1 + \
            row['visionScorePerMinute'] * 0.1 + row['damageTakenOnTeamPercentage'] * 0.2 + row['damagePerMinute'] * 0.2
    # remove kda
    elif row['position'] == 'MID':
        return row['damagePerMinute'] * 0.3 + row['kda'] * 0.3 + row['killParticipation'] * 0.2 + \
            row['CSPerMinute'] * 0.1 + row['teamDamagePercentage'] * 0.1
    # add damageObjectif
    elif row['position'] == 'BOT':
        return row['CSPerMinute'] * 0.2 + row['kda'] * 0.3 + row['damagePerMinute'] * 0.2 + \
            row['killParticipation'] * 0.2 + row['teamDamagePercentage'] * 0.1
    # replace CS by gold
    elif row['position'] == 'SUP':
        return row['visionScorePerMinute'] * 0.3 + row['killParticipation'] * 0.3 + row['totalHealsOnTeammates'] * 0.2 \
            + row['totalDamageShieldedOnTeammates'] * 0.2
    # add CC score
    else:
        return 0  # default value