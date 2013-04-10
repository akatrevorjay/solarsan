import urwid


class MenuButton(urwid.Button):
    def __init__(self, caption, callback):
        super(MenuButton, self).__init__("")
        urwid.connect_signal(self, 'click', callback)
        self._w = urwid.AttrMap(urwid.SelectableIcon(
            [u'  \N{BULLET} ', caption], 2), None, 'selected')


class SubMenu(urwid.WidgetWrap):
    def __init__(self, caption, choices):
        super(SubMenu, self).__init__(MenuButton(
            [caption, u"\N{HORIZONTAL ELLIPSIS}"], self.open_menu))
        line = urwid.Divider(u'\N{LOWER ONE QUARTER BLOCK}')
        listbox = urwid.ListBox(urwid.SimpleFocusListWalker([
            urwid.AttrMap(urwid.Text([u"\n  ", caption]), 'heading'),
            urwid.AttrMap(line, 'line'),
            urwid.Divider()] + choices + [urwid.Divider()]))
        self.menu = urwid.AttrMap(listbox, 'options')

    def open_menu(self, button):
        bottom_boxes.open_box(self.menu)


class Choice(urwid.WidgetWrap):
    def __init__(self, caption):
        super(Choice, self).__init__(
            MenuButton(caption, self.item_chosen))
        self.caption = caption

    def item_chosen(self, button):
        response = urwid.Text([u'  You chose ', self.caption, u'\n'])
        done = MenuButton(u'Ok', exit_program)
        response_box = urwid.Filler(urwid.Pile([response, done]))
        bottom_boxes.open_box(urwid.AttrMap(response_box, 'options'))


def exit_program(key):
    raise urwid.ExitMainLoop()


palette = [
    (None,  'light gray', 'black'),
    ('heading', 'black', 'light gray'),
    ('line', 'black', 'light gray'),
    ('options', 'dark gray', 'black'),
    ('focus heading', 'white', 'dark red'),
    ('focus line', 'black', 'dark red'),
    ('focus options', 'black', 'light gray'),
    ('selected', 'white', 'dark blue')]
focus_map = {
    'heading': 'focus heading',
    'options': 'focus options',
    'line': 'focus line'}


class HorizontalBoxes(urwid.Columns):
    def __init__(self):
        super(HorizontalBoxes, self).__init__([], dividechars=1)

    def open_box(self, box):
        if self.contents:
            del self.contents[self.focus_position + 1:]
        self.contents.append((urwid.AttrMap(box, 'options', focus_map),
                              self.options('given', 30),
                              ))
        self.focus_position = len(self.contents) - 1


from solarsan.storage.device import Devices, Disk, Partition, Mirror
devices = Devices()



vdevs = [devices[0], Mirror([devices[1], devices[2]])]
vdevs_choices = [Choice(vdev.__repr__()) for vdev in vdevs]

selected_devices = {}

devices_choices = [Choice(dev.__repr__()) for dev in devices if not dev.is_readonly]


add_device_menu = SubMenu(u'Add device', devices_choices)
remove_device_menu = SubMenu(u'Remove device', devices_choices)


m_vdev_type_mirror = SubMenu(u'Mirror', [
    add_device_menu,
    remove_device_menu,
])


vdev_types = ['Mirror']
vdev_types_choices = [Choice(x) for x in vdev_types]


m_vdev_type_select = SubMenu(u'Select vdev type', [
    m_vdev_type_mirror,
])


m_vdev_add = SubMenu(u'Add vdev', vdev_types_choices)
m_vdev_select = SubMenu(u'Select vdev', vdevs_choices)
m_vdev_edit = SubMenu(u'Edit vdev', vdevs_choices)
m_vdev_remove = SubMenu(u'Remove vdev', vdevs_choices)


m_vdevs = SubMenu(u'Devices', [
    m_vdev_edit,
    m_vdev_add,
    m_vdev_remove,
])



menu_top = SubMenu(u'Pool', [
    m_vdevs,
    Choice('Rename'),
    Choice('Save and create Pool'),
])


pool_text = [
    ('bold', u'> Create Pool: '), u'dpool', u'\n',
    u'\n',

    ('bold', u'Devices: ['), u'\n',
    u'    %s' % devices[0], u'\n',
    ('bold', u']'), u'\n',

    u'\n',
]

top_text = urwid.Text(pool_text)
top_text_fill = urwid.Filler(top_text, 'top', min_height=20, top=1, bottom=1)
top_text_pad = urwid.Padding(top_text_fill, left=2, right=2)

#top_text_pad = urwid.Padding(top_text, left=2, right=2)
#top_text_box = urwid.BoxAdapter(top_text, 20)

top_cols = (20, urwid.Columns([top_text_pad]))
#top_cols = urwid.Columns([top_text_pad], dividechars=1)
#top_cols = top_text
#top_cols = ('pack', urwid.Columns([]))
#top_cols = ('pack', top_text)
#top_cols = top_text_fill
#top_cols = urwid.LineBox(top_text_fill)




bottom_boxes = HorizontalBoxes()
bottom_boxes.open_box(menu_top.menu)
#bottom_cols = urwid.Filler(bottom_boxes, 'middle', 20)
bottom_cols = bottom_boxes

#top = urwid.Pile([top_cols, bottom_cols])
top_pile = urwid.Pile([top_cols, bottom_cols])
#top_pile = urwid.Pile([top_cols])

#top_lb = urwid.ListBox([top_cols, bottom_cols])
#top_lb_box = urwid.BoxAdapter(top_lb, 20)

#top = top_lb
#top = urwid.Filler(bottom_cols, 'middle', 30)
#top = bottom_cols
#top = urwid.ListBox([bottom_cols])
top = top_pile


#urwid.MainLoop(urwid.Filler(top, 'middle', 30), palette).run()
urwid.MainLoop(top, palette,).run()




