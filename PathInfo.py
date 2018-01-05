
class Info:

    tableinfo = {
        'action':{'cols':{'userid':'INT UNSIGNED NOT NULL',
                          'actionType':'INT(2) UNSIGINED',
                          'actionDate':'INT(8) UNSIGNED',
                          'actionTime':'INT(6) UNSIGNED'},
                  'prmkey':['userid','actionDate','actionTime']},

        'orderHistory':{'cols':{'userid':'INT UNSIGNED NOT NULL',
                                'orderid':'INT UNSIGNED NOT NULL',
                                'orderDate':'INT(8) UNSIGNED',
                                'orderTime':'INT(6) UNSIGNED',
                                'orderType':'INT(2) UNSIGNED',
                                'city':'TEXT',
                                'country':'TEXT',
                                'continent':'TEXT'},
                        'prmkey':['orderid']},

        'orderFuture':{'cols':{'userid':'INT UNSIGNED NOT NULL',
                               'orderType':'INT(2) UNSIGNED'},
                       'prmkey':['userid','orderType']},

        'userComment':{'cols':{'userid':'INT UNSIGNED NOT NULL',
                               'orderid':'INT UNSIGNED NOT NULL',
                               'rating':'FLOAT',
                               'tags':'TEXT',
                               'commentsKeyWords':'TEXT'},
                       'prmkey':['orderid']},

        'userProfile':{'cols':{'userid':'INT UNSIGNED NOT NULL',
                               'gender':'TEXT',
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
