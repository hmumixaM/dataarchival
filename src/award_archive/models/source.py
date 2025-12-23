"""Source enum for mileage program sources."""

from enum import Enum


class Source(str, Enum):
    """Available mileage program sources from Seats.aero."""
    
    # All 24 mileage programs from seats.aero
    aeromexico = "aeromexico"              # Aeromexico Club Premier
    aeroplan = "aeroplan"                  # Air Canada Aeroplan
    flying_blue = "flying_blue"            # Air France/KLM Flying Blue
    alaska = "alaska"                      # Alaska Mileage Plan
    american = "american"                  # American Airlines AAdvantage
    azul = "azul"                          # Azul Fidelidade
    connectmiles = "connectmiles"          # Copa ConnectMiles
    delta_skymiles = "delta_skymiles"      # Delta SkyMiles
    emirates = "emirates"                  # Emirates Skywards
    ethiopian = "ethiopian"                # Ethiopian ShebaMiles
    etihad = "etihad"                      # Etihad Guest
    finnair = "finnair"                    # Finnair Plus
    smiles = "smiles"                      # GOL Smiles
    jetblue = "jetblue"                    # JetBlue TrueBlue
    lufthansa = "lufthansa"                # Lufthansa Miles & More
    qantas = "qantas"                      # Qantas Frequent Flyer
    qatar = "qatar"                        # Qatar Airways Privilege Club
    eurobonus = "eurobonus"                # SAS EuroBonus
    saudia = "saudia"                      # Saudia AlFursan
    singapore = "singapore"                # Singapore Airlines KrisFlyer
    turkish = "turkish"                    # Turkish Miles&Smiles
    united = "united"                      # United MileagePlus
    virgin_atlantic = "virgin_atlantic"    # Virgin Atlantic Flying Club
    velocity = "velocity"                  # Virgin Australia Velocity


ALL_SOURCES = [s.value for s in Source]

