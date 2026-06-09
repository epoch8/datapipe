Прямое доказательство из логов
По post_filter_to_adv_detection_moderate за весь 2.5-часовой прогон:

Offset not calculated for post_filter_to_adv_detection_moderate_cb2cba6692: output is empty (0 rows processed)   ← на КАЖДОМ из 19 батчей
Offsets not updated for post_filter_to_adv_detection_moderate_cb2cba6692: changes.offsets is empty after processing 19 batches
То же самое для post_filter_to_moderate, filter_post_specifically_recommend, label_studio_output_to_recommendations, block_label_studio_output и др. Offset не записывается вообще.

Механизм бага (точно по коду datapipe/step/batch_transform.py)
После обработки батча новый offset для входа считается по ключам ВЫХОДА: processed_idx = data_to_index(first_output, transform_keys), затем max(update_ts) по мета-таблице входа, отфильтрованной этими ключами (_get_max_update_ts_for_batch, стр. 599–618).
Если у батча выход пустой (first_output.empty) — offset для батча не считается (стр. 626–631), пишется warning.
Offset коммитится в transform_input_offsets только если changes.offsets непустой (стр. 825–833).
post_filter_to_moderate — это фильтр/роутер: из чанка post_input он оставляет только status=='on_moderation' + переписывает legacy-строки. Подавляющее большинство батчей changelist'а (обычные апдейты постов, не на модерации) → output пустой → offset не считается. Раз пустой на всех батчах → changes.offsets пустой → offset не двигается с Feb 17. Следующий прогон снова берёт Feb 17 → строит changelist на 3.5 млн строк → 5.5 ч. Самоподдерживающийся цикл.

Суть дефекта: offset должен продвигаться по обработанному ВХОДУ (что прочитано/consumed), а не по ВЫХОДУ (что произведено). «Пустой выход» ≠ «ничего не обработано» — для фильтров это норма. Строки же реально помечаются обработанными (mark_rows_processed_success, стр. 587), но offset за ними не следует.

Итоговая цепочка (всё подтверждено)
use_offset_optimization=True на фильтр-трансформах → пустой output на батчах → offset не коммитится → застывает (Feb 17 / Mar 23) → каждый 5-минутный (!) запуск CronJob (*/5, Forbid) пересканирует месяцы изменений post_input (63 GB / 26 млн) → один прогон 8 ч → следующие ~100 расписаний пропускаются → так по кругу.