from sqlalchemy import func

insert_demo_data = [
    [1000, 'First Aid Kit', func.to_date('2017-03-05-07-34-07', 'yyyy-mm-dd-hh24-mi-ss')],
    [1001, 'Skin Brightening Gel', func.to_date('1991-01-12-12-45-06', 'yyyy-mm-dd-hh24-mi-ss')],
    [1002, 'Gel Eye Mask', func.to_date('1988-12-17-09-34-30', 'yyyy-mm-dd-hh24-mi-ss')],
    [1003, 'Soy Formula', func.to_date('1991-03-05-10-09-07', 'yyyy-mm-dd-hh24-mi-ss')],
    [1004, 'Horsehound', func.to_date('1988-12-05-07-23-43', 'yyyy-mm-dd-hh24-mi-ss')],
    [1005, 'Rose Hips', func.to_date('2017-11-20-09-53-30', 'yyyy-mm-dd-hh24-mi-ss')],
    [1006, 'Hot Rollers', func.to_date('1988-03-17-10-09-43', 'yyyy-mm-dd-hh24-mi-ss')],
    [1007, 'Straight Waver', func.to_date('2017-11-20-09-16-07', 'yyyy-mm-dd-hh24-mi-ss')],
    [1008, 'Hand Lotion', func.to_date('1991-12-07-07-09-12', 'yyyy-mm-dd-hh24-mi-ss')],
    [1009, 'Quick Braid', func.to_date('2017-11-17-10-34-43', 'yyyy-mm-dd-hh24-mi-ss')]
]