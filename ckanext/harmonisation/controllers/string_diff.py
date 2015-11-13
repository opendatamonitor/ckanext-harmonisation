from difflib import ndiff

def comp(s1,s2):
    del_diffs = []
    add_diffs = []

    for i,s in enumerate(ndiff(s1,s2)):
        if s[0]==' ': continue
        elif s[0]=='-':
            # print(u'Delete "{}" from position {}'.format(s[-1],i))
            del_diffs.append({'pos':i,'char':s[-1]})
        elif s[0]=='+':
            # print(u'Add "{}" to position {}'.format(s[-1],i))
            add_diffs.append({'pos':i,'char':s[-1]})

    return (del_diffs,add_diffs)
