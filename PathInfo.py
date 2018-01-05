
class Info:

    tableinfo = {
        'action':{'cols':{'userid':'BIGINT UNSIGNED NOT NULL',
                          'actionType':'INT(2) UNSIGNED',
                          'actionTime':'INT UNSIGNED'},
                  'prmkey':['userid','actionTime']},

        'orderHistory':{'cols':{'userid':'BIGINT UNSIGNED NOT NULL',
                                'orderid':'INT UNSIGNED NOT NULL',
                                'orderTime':'INT UNSIGNED',
                                'orderType':'INT(2) UNSIGNED',
                                'city':'TEXT',
                                'country':'TEXT',
                                'continent':'TEXT'},
                        'prmkey':['orderid']},

        'orderFuture':{'cols':{'userid':'BIGINT UNSIGNED NOT NULL',
                               'orderType':'INT(2) UNSIGNED'},
                       'prmkey':['userid','orderType']},

        'userComment':{'cols':{'userid':'BIGINT UNSIGNED NOT NULL',
                               'orderid':'INT UNSIGNED NOT NULL',
                               'rating':'FLOAT',
                               'tags':'TEXT',
                               'commentsKeyWords':'TEXT'},
                       'prmkey':['orderid']},

        'userProfile':{'cols':{'userid':'BIGINT UNSIGNED NOT NULL',
                               'gender':'TEXT' ,
                               'province':'TEXT',
                               'age':'TEXT'},
                       'prmkey':['userid']}
    }


class LogMark:
    critical = '[=]'
    error = '[-]'
    warning = '[!]'
    info = '[+]'
    debug = '[*]'
