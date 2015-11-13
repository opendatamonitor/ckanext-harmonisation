#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Description:
Parsing locale specific dates in free form format to datetime format
Actually, this is just a wrapper around dateutil with a kind of i18n support

History:
   * 2010-04-17T10:44:30+0400 Initial commit. Version 0.1 released.
"""

__author__ = 'Nikolay Panov (author@niksite.ru)'
__license__ = 'GPLv3'
__version__ = 0.22
__updated__ = '2010-04-21 21:15:55 nik'

import dateutil.parser
import logging
import re



DATETIME_VALID_NONCHAR_SYMBOLS = u'\-|\—|\+|\/|\s|\d|\:|\,|\.|T|Z'
DATETIME_VALIDATION_REGEX = re.compile(u'%s|gennaio|febbraio|marzo|luglio|agosto|settembre|ottobre|novembre|dicembre|venerdì|lunedì|martedì|mercoledì|giovedì|sabato|domenica|mi茅rcoles|aprile|giugno|făšvrier|ottobre|maggio|luglio|gennaio|venerdăź|diciembre|dicembre|miăšrcoles|martedăź|săąbado|lunedăź|mercoledăź|giovedăź|at|понедельник|воскресенье|septiembre|donnerstag|september|diciembre|miércoles|September|Wednesday|noviembre|septembre|Thursday|dienstag|vendredi|novembre|november|December|сентября|November|February|mercredi|mittwoch|dimanche|dezember|décembre|Saturday|августа|janvier|samstag|пятница|суббота|декабря|octubre|freitag|вторник|juillet|viernes|Tuesday|sonntag|février|febrero|октября|oktober|October|January|февраля|octobre|domingo|четверг|februar|Sunday|января|montag|august|agosto|апреля|Friday|August|Monday|ноября|jueves|martes|januar|samedi|sábado|mardi|March|enero|jeudi|среда|nachm|junio|april|julio|abril|avril|lundi|lunes|марта|April|июля|juil|mayo|juni|févr|July|нояб|февр|sept|janv|June|mars|juli|märz|vorm|июня|août|juin|сент|янв|авг|Wed|sáb|mär|Aug|aug|jan|Mon|ven|Nov|oct|okt|nov|dim|dic|Sat|мая|sep|Sun|Oct|abr|a.m|Tue|lun|mer|дек|Feb|vie|apr|Fri|Jun|mié|feb|jeu|Jul|Jan|окт|jun|jul|avr|jue|Dec|dez|Sep|Apr|sam|апр|p.m|Mar|ene|May|mar|may|Thu|mai|ago|dom|déc|do|чт|вт|пн|PM|pm|mo|mi|am|di|сб|пт|ср|fr|AM|so|sa|вс|' % DATETIME_VALID_NONCHAR_SYMBOLS, re.UNICODE | re.IGNORECASE)
TRANSLATION_DICT = {
    u'a.m': u'AM',
    u'abr': u'Apr',
    u'abril': u'April',
    u'ago': u'Aug',
    u'agosto': u'August',
    u'am': u'AM',
    u'août': u'Aug',
    u'apr': u'Apr',
    u'april': u'April',
    u'aug': u'Aug',
    u'august': u'August',
    u'avr': u'Apr',
    u'avril': u'April',
    u'dez': u'Dec',
    u'dezember': u'December',
    u'di': u'Tue',
    u'dic': u'Dec',
    u'diciembre': u'December',
    u'dienstag': u'Tuesday',
    u'dim': u'Sun',
    u'dimanche': u'Sunday',
    u'do': u'Thu',
    u'dom': u'Sun',
    u'domingo': u'Sunday',
    u'donnerstag': u'Thursday',
    u'déc': u'Dec',
    u'décembre': u'December',
    u'ene': u'Jan',
    u'enero': u'January',
    u'feb': u'Feb',
    u'febrero': u'February',
    u'februar': u'February',
    u'fr': u'Fri',
    u'freitag': u'Friday',
    u'févr': u'Feb',
    u'février': u'February',
    u'jan': u'Jan',
    u'januar': u'January',
    u'janv': u'Jan',
    u'janvier': u'January',
    u'jeu': u'Thu',
    u'jeudi': u'Thursday',
    u'jue': u'Thu',
    u'jueves': u'Thursday',
    u'juil': u'Jul',
    u'juillet': u'July',
    u'juin': u'Jun',
    u'jul': u'Jul',
    u'juli': u'July',
    u'julio': u'July',
    u'jun': u'Jun',
    u'juni': u'June',
    u'junio': u'June',
    u'lun': u'Mon',
    u'lundi': u'Monday',
    u'lunes': u'Monday',
    u'mai': u'May',
    u'mar': u'Tue',
    u'mardi': u'Tuesday',
    u'mars': u'Mar',
    u'martes': u'Tuesday',
    u'marzo': u'March',
    u'may': u'May',
    u'mayo': u'May',
    u'mer': u'Wed',
    u'mercredi': u'Wednesday',
    u'mi': u'Wed',
    u'mittwoch': u'Wednesday',
    u'mié': u'Wed',
    u'miércoles': u'Wednesday',
    u'mo': u'Mon',
    u'montag': u'Monday',
    u'mär': u'Mar',
    u'märz': u'March',
    u'nachm': u'PM',
    u'nov': u'Nov',
    u'november': u'November',
    u'novembre': u'November',
    u'noviembre': u'November',
    u'oct': u'Oct',
    u'octobre': u'October',
    u'octubre': u'October',
    u'okt': u'Oct',
    u'oktober': u'October',
    u'p.m': u'PM',
    u'pm': u'PM',
    u'sa': u'Sat',
    u'sam': u'Sat',
    u'samedi': u'Saturday',
    u'samstag': u'Saturday',
    u'sep': u'Sep',
    u'sept': u'Sep',
    u'september': u'September',
    u'septembre': u'September',
    u'septiembre': u'September',
    u'so': u'Sun',
    u'sonntag': u'Sunday',
    u'sáb': u'Sat',
    u'sábado': u'Saturday',
    u'ven': u'Fri',
    u'vendredi': u'Friday',
    u'vie': u'Fri',
    u'viernes': u'Friday',
    u'vorm': u'AM',
    u'авг': u'Aug',
    u'августа': u'August',
    u'апр': u'Apr',
    u'апреля': u'April',
    u'воскресенье': u'Sunday',
    u'вс': u'Sun',
    u'вт': u'Tue',
    u'вторник': u'Tuesday',
    u'дек': u'Dec',
    u'декабря': u'December',
    u'июля': u'Jul',
    u'июня': u'Jun',
    u'марта': u'Mar',
    u'мая': u'May',
    u'нояб': u'Nov',
    u'ноября': u'November',
    u'окт': u'Oct',
    u'октября': u'October',
    u'пн': u'Mon',
    u'понедельник': u'Monday',
    u'пт': u'Fri',
    u'пятница': u'Friday',
    u'сб': u'Sat',
    u'сент': u'Sep',
    u'сентября': u'September',
    u'ср': u'Wed',
    u'среда': u'Wednesday',
    u'суббота': u'Saturday',
    u'февр': u'Feb',
    u'февраля': u'February',
    u'четверг': u'Thursday',
    u'чт': u'Thu',
    u'янв': u'Jan',
    u'января': u'January',
    u'miăšrcoles':'',
    u'martedăź':'',
    u'săąbado':'',
    u'lunedăź':'',
    u'giovedăź':'',
    u'mercoledăź':'',
    u'domenica':'',
    'dicembre':'December',
    'diciembre':'December',
    'mi茅rcoles':'',
    'venerdăź':'',
    'gennaio':'January',
    'luglio':'July',
    'ottobre':'October',
    'făšvrier':'February',
    'febbraio':'February',
    u'giugno':'June',
    u'aprile':'April',
    u'maggio':'May',
    u'lunedì':'Monday',
    u'martedì':'Tuesday',
    u'mercoledì':'Wednesday',
    u'giovedì':'Thursday',
    u'venerdì':'Friday',
    u'sabato':'Saturday',
    u'Domenica':'Sunday'

}


def _generate_translation_dict(self_update=False):
    """Generate translation dictionary object
    If self_update is True, update this file to store generated values"""
    import os
    import PyICU

    english_locale = PyICU.Locale('en_US')
    english_date_format = PyICU.DateFormatSymbols(english_locale)
    get_func_names = filter(lambda name: re.match('^get[AMWS]\w+s$', name), dir(english_date_format))
    for locale in 'ru_RU de_DE fr_FR es_ES'.split():
        this_locale = PyICU.Locale(locale)
        local_date_format = PyICU.DateFormatSymbols(this_locale)
        for func_name in get_func_names:
            TRANSLATION_DICT.update(dict(zip(map(lambda word: word.lower().strip('.'), getattr(local_date_format, func_name)()), getattr(english_date_format, func_name)())))
    if '' in TRANSLATION_DICT:
        del TRANSLATION_DICT['']
    translation_dict_str = "TRANSLATION_DICT = {"
    for k, v in sorted(TRANSLATION_DICT.items()):
        translation_dict_str += "\n    u'%s': u'%s'," % (k, v)
    translation_dict_str = translation_dict_str.strip(',') + '}'
    datetime_regex_str = u"DATETIME_VALIDATION_REGEX = re.compile(u'%%s|at|%s' %% DATETIME_VALID_NONCHAR_SYMBOLS, re.UNICODE | re.IGNORECASE)" % '|'.join(sorted(set(TRANSLATION_DICT.keys() + TRANSLATION_DICT.values()), cmp=lambda a, b: cmp(len(b), len(a))))
    if self_update:
        this_file_data = unicode(''.join(open(__file__).readlines()), encoding='utf-8')
        this_file_data = re.sub(u'TRANSLATION_DICT = {[^}]*}', translation_dict_str, this_file_data, 1)
        this_file_data = re.sub(u'DATETIME_VALIDATION_REGEX = re.compile\(.*?, re.UNICODE\)', datetime_regex_str, this_file_data, 1)
        temp_file_name = '%s.new' % __file__  # using temp file is a bit paranoid, I know
        temp_file = open(temp_file_name, 'w')
        temp_file.write(this_file_data.encode('utf-8'))
        temp_file.close()
        os.rename(temp_file_name, __file__)
    else:
        return translation_dict_str


def _prepare_date_string(possible_date):
    """This function check the given string for whether it is possible a date.
    If so, prepare the string for parsing."""
    if not isinstance(possible_date, unicode):
        possible_date = possible_date.decode('utf-8', 'ignore')
    possible_date = re.sub(r'<[^>]*?>|&#\d+;', ' ', possible_date)  # strip all html tags
    possible_date = re.sub(re.compile(u'\s+', re.UNICODE), u' ', possible_date)
    possible_date = re.sub(re.compile(u'\W+$', re.UNICODE), '', possible_date)
    if 5 <= len(possible_date) <= 50:
        possible_date = re.sub(re.compile('posted on|at|by', re.IGNORECASE), '', possible_date)
        if possible_date and not DATETIME_VALIDATION_REGEX.sub('', possible_date):
            return possible_date
    return None


def parse(date_string, force_parse=False, force_update=False, loglevel=logging.ERROR):
    """Got date_string -- free form locale specific date sting
    Output: datetime.datetime object or None"""
    # let's setup logger
    date_string=date_string.lower()
    date_string=date_string
    logger = logging.getLogger('date_parser')
    logger.setLevel(loglevel)
    logger.manager.emittedNoHandlerWarning = True
    # let's start the work at last
    date_string = _prepare_date_string(date_string)
    if not date_string:
        # the given string does not looks like string with date
        return None
    date_invalid_symbol = re.compile(u'[^%s\w]' % DATETIME_VALID_NONCHAR_SYMBOLS, re.UNICODE)
    #non_english_word = re.compile(u'[^%sa-zA-Z]+' % DATETIME_VALID_NONCHAR_SYMBOLS, re.UNICODE)
    non_english_word = re.compile(u'[^%s]+' % DATETIME_VALID_NONCHAR_SYMBOLS, re.UNICODE)
    if not TRANSLATION_DICT or force_update:
        _generate_translation_dict(self_update=force_update)
    if date_invalid_symbol.search(date_string):
        logger.debug('we have unexpected symbols "%s" into the date_string "%s"' % (date_invalid_symbol.search(date_string).group(), date_string))
        if force_parse:
            logger.debug('drop all unexpected symbols out')
            date_string = date_invalid_symbol.sub('', date_string)
        else:
            logger.debug('no force, so just return None')
            return None
    try:
# remove Z from the end of string when the time is in TZ
        if date_string[-1] == 'z':
            date_string = date_string[:-1]
        # first shot
        return dateutil.parser.parse(date_string)
    except Exception:
        # no free bananas
        if non_english_word.search(date_string):
            # let's try to translate this text to English
            for word in non_english_word.findall(date_string):
                if word.lower() not in TRANSLATION_DICT:
                    logger.warning('cannot translate word "%s" into the string "%s"' % (word, date_string))
                    return None
                date_string = date_string.replace(word, TRANSLATION_DICT[word.lower()])
            logger.debug('date_string has been translated to "%s"' % date_string)
            try:
                # second shot
                return dateutil.parser.parse(date_string)
            except Exception, e:
                # bad luck
                logger.warning('cannot translate date_string "%s": %s' % (date_string, repr(e)))
        return None


if __name__ == '__main__':
    from optparse import OptionParser

    logging.basicConfig()
    parser = OptionParser(usage="%prog [-f] [-q] <free-form date string>", version=str(__version__))
    parser.add_option("-f", "--force-update",
                      action='store_true', dest="force_update", default=False,
                      help="force self-updating of TRANSLATION_DICT in this file", metavar="FILE")
    parser.add_option("-q", "--quiet",
                      action="store_false", dest="verbose", default=True,
                      help="don't print debug messages to stdout")
    (options, args) = parser.parse_args()
    if len(args) != 1:
        parser.error("you have missed query string")
    date_string = args[0]
    if options.verbose:
        print '%s -> %s' % (date_string, parse(date_string, loglevel=logging.DEBUG, force_update=options.force_update))
    else:
        print parse(date_string, loglevel=logging.WARNING, force_update=options.force_update)

# Emacs:
# Local variables:
# time-stamp-pattern: "100/^__updated__ = '%%'$"
# End:
