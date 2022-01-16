import yaml
from Data.Utils import pgyaml
data = {
    'literal': pgyaml.literal_unicode(
        u'by hjw              ___\n'
         '   __              /.-.\\\n'
         '  /  )_____________\\\\  Y\n'
         ' /_ /=== == === === =\\ _\\_\n'
         '( /)=== == === === == Y   \\\n'
         ' `-------------------(  o  )\n'
         '                      \\___/\n'),
    'folded': pgyaml.folded_unicode(
        u'It removes all ordinary curses from all equipped items. '
        'Heavy or permanent curses are unaffected.\n')}


data = {
    'literal': pgyaml.literal_unicode("aa")


}

if __name__ == "__main__":
    with open("test.txt", "r") as f:
        #print(f.read())
        print(f"useful example: {yaml.dump(pgyaml.literal_unicode(f.read()))}")

